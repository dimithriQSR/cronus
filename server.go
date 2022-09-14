package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"sync"

	"qsrconnector/qsrazureb2c"
	"qsrconnector/qsrkeystore"
	qsr "qsrconnector/qsrmessage"
	"reflect"
	"strconv"

	"github.com/MicahParks/keyfunc"
	"github.com/jackc/pgproto3/v2"
	"github.com/jackc/pgtype"
	pgtypeuuid "github.com/jackc/pgtype/ext/gofrs-uuid"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
)

type Configuration struct {
	Cloudconnect                string
	Localconnect                string
	CertKey                     string
	CertCrt                     string
	TLSport                     int32
	NonTLSport                  int32
	InsecureSkipVerify          bool
	Minpool                     int32
	Maxpool                     int32
	MaxMsgSize                  int
	AzureAdInstance             string
	AzureAdDomain               string
	AzureAdTenantId             string
	AzureAdClientId             string
	AzureAdAudience             string
	AzureAdSignUpSignInPolicyId string
	EnableTokenCheck            bool
	DebugQuery                  bool
	GrpcReflection              bool
}

const (
	clouddb           = "cloud"
	localdb           = "local"
	execinput         = 1
	execnoreturninput = 2
	queryinput        = 3
)

var (
	mystor           = qsrkeystore.KeyMap{}
	jwskin           *keyfunc.JWKS
	enabletokencheck = true
	typemap          = []string{"int", "int64", "int32", "float", "float32", "float64", "bool"}
	debugsql         = false
)

func NewMessageServer() *MessageServer {
	return &MessageServer{}
}

func (s *MessageServer) PingServer(ctx context.Context, in *qsr.MessageRequest) (*qsr.MessageReply, error) {
	if debugsql {
		log.Printf("Received: %v", in.GetMessage())
	} else {
		log.Printf("Received: Query")
	}
	return &qsr.MessageReply{Message: "Ping " + in.GetMessage()}, nil
}

func (s *MessageServer) ExecStatement(ctx context.Context, in *qsr.QueryRequest) (*qsr.QueryReply, error) {
	if debugsql {
		log.Printf("Received: %v", in.GetMessage())
	} else {
		log.Printf("Received: Query")
	}
	connid := []string{clouddb, localdb}
	return s.execByConn(in, &connid, execinput)
}

func (s *MessageServer) ExecStreamStatement(in *qsr.QueryRequest, sr qsr.MessageService_ExecStreamStatementServer) error {
	connid := []string{clouddb, localdb}
	ProcesStream(s, in, sr, &connid, execinput)
	return nil
}

func (s *MessageServer) ExecStatementNoReturn(ctx context.Context, in *qsr.QueryRequest) (*qsr.QueryReply, error) {
	if debugsql {
		log.Printf("Received: %v", in.GetMessage())
	} else {
		log.Printf("Received: Execution")
	}
	connid := []string{clouddb, localdb}
	go s.execByConn(in, &connid, execnoreturninput)
	ret := qsr.QueryReply{}
	return &ret, nil
}

func (s *MessageServer) QueryStatement(ctx context.Context, in *qsr.QueryRequest) (*qsr.QueryReply, error) {
	if debugsql {
		log.Printf("Received: %v", in.GetMessage())
	} else {
		log.Printf("Received: Query")
	}
	connid := []string{clouddb}
	return s.execByConn(in, &connid, queryinput)
}

func (s *MessageServer) QueryLocalStatement(ctx context.Context, in *qsr.QueryRequest) (*qsr.QueryReply, error) {
	if debugsql {
		log.Printf("Received: %v", in.GetMessage())
	} else {
		log.Printf("Received: Query")
	}
	connid := []string{localdb}
	return s.execByConn(in, &connid, queryinput)
}

func (s *MessageServer) QueryStreamStatement(in *qsr.QueryRequest, sr qsr.MessageService_QueryStreamStatementServer) error {
	connid := []string{clouddb}
	ProcesStream(s, in, sr, &connid, queryinput)
	return nil
}

func (s *MessageServer) QueryStreamLocalStatement(in *qsr.QueryRequest, sr qsr.MessageService_QueryStreamLocalStatementServer) error {
	connid := []string{localdb}
	ProcesStream(s, in, sr, &connid, queryinput)
	return nil
}

func ProcesStream(s *MessageServer, in *qsr.QueryRequest, sr qsr.MessageService_ExecStreamStatementServer, connid *[]string, exinp int) {
	if debugsql {
		log.Printf("Received: %v", in.GetMessage())
	} else {
		log.Printf("Received: Query")
	}
	var reply *qsr.QueryReply
	//use wait group to allow process to be concurrent
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		reply, _ = s.execByConn(in, connid, exinp)
		if err := sr.Send(reply); err != nil {
			log.Printf("send error %v", err)
		}
	}()
	wg.Wait()
}

func (s *MessageServer) execByConn(in *qsr.QueryRequest, connid *[]string, exectype int) (*qsr.QueryReply, error) {
	var errout error
	bytesreturn := []byte{}
	for _, v := range *connid {
		if s.conn[v] != nil {
			if exectype == queryinput || exectype == execinput {
				// return data for execinput
				sql := in.GetMessage()
				if exectype == execinput {
					sql = fmt.Sprintf("%s RETURNING *", in.GetMessage())
				}
				log.Printf("sql statement: %v\n", sql)
				rows, err := s.conn[v].Query(context.Background(), sql)
				if err != nil {
					fmt.Println("issue with execution of query statement", in.GetMessage(), err)
					errout = err
				} else {
					bytesreturn = append(bytesreturn, []byte("[")...)
					head := rows.FieldDescriptions()
					for rows.Next() {
						rowvalue, err := rows.Values()
						if err == nil {
							retv := make(chan string, 1)
							go createNameValuePair(head, rowvalue, retv)
							val := <-retv
							if len(bytesreturn) != 1 {
								val = "," + val
							}
							bytesreturn = append(bytesreturn, []byte(val)...)
						}
					}
					bytesreturn = append(bytesreturn, []byte("]")...)
				}
				if rows != nil {
					defer rows.Close()
				}
			} else if exectype == execnoreturninput {
				sql := in.GetMessage()
				log.Printf("sql statement: %v\n", sql)
				_, err := s.conn[v].Exec(context.Background(), sql)
				if err != nil {
					fmt.Println("issue with execution of exec statement", in.GetMessage(), err)
					errout = err
				}
			}
		}
	}
	return &qsr.QueryReply{Message: "Excuted " + in.GetMessage(), Data: bytesreturn}, errout
}

func createNameValuePair(headin []pgproto3.FieldDescription, value []interface{}, returnval chan string) {
	valret := `{`
	for i := range headin {

		headval := string((headin)[i].Name)
		inval := fmt.Sprintf("%v", (value)[i])
		if inval == "<nil>" {
			inval = ""
		}
		thevalue := (value)[i]
		//	aa := thevalue
		//fmt.Print(aa)
		typeval := ""
		if thevalue != nil {
			typeval = (reflect.TypeOf(thevalue)).Name()
			if typeval == "" {
				inval = string(thevalue.([]uint8))
			}
		}
		valret = valret + `"` + headval + `":`
		if !contains(typemap, typeval) {
			valret = valret + `"` + strings.ReplaceAll(strings.ReplaceAll(strings.ReplaceAll(strings.ReplaceAll(string(inval), "\\", "\\\\"), "\"", "\\\""), "\r", "\\r"), "\n", "\\n") + `",`
		} else {
			valret = valret + inval + `,`
		}
	}
	valret = valret[:len(valret)-1] + `}`
	returnval <- valret
}

func contains(s []string, str string) bool {
	for _, v := range s {
		if v == str {
			return true
		}
	}
	return false
}

func loadTLSCredentials(cert, key string, kverify bool) (credentials.TransportCredentials, error) {
	// Load server's certificate and private key
	serverCert, err := tls.LoadX509KeyPair(cert, key)
	if err != nil {
		return nil, err
	}

	// Create the credentials and return it
	config := &tls.Config{
		Certificates:       []tls.Certificate{serverCert},
		InsecureSkipVerify: kverify,
		ClientAuth:         tls.NoClientCert,
	}

	return credentials.NewTLS(config), nil
}

type MessageServer struct {
	conn map[string]*pgxpool.Pool
	qsr.UnimplementedMessageServiceServer
}

func unaryInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	var err error
	var goodkey bool
	if enabletokencheck {
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			return req, status.Errorf(codes.InvalidArgument, "Retrieving metadata is failed")
		}
		if len(md["token"]) > 0 {
			goodkey, err = qsrazureb2c.CheckToken(&mystor, md["token"][0], jwskin)
			if !goodkey {
				if err != nil {
					return ctx, status.Errorf(codes.PermissionDenied, " token issue good bye ", err.Error())
				}
			}
		} else {
			req = status.Errorf(codes.Unauthenticated, "the token is not supplied, go away")
		}
	}
	log.Println("--> unary interceptor: ", info.FullMethod)
	return handler(ctx, req)
}

func streamInterceptor(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	var err error
	var goodkey bool
	if enabletokencheck {
		md, ok := metadata.FromIncomingContext(stream.Context())
		if !ok {
			return status.Errorf(codes.InvalidArgument, "Retrieving metadata is failed")
		}
		if len(md["token"]) > 0 {
			goodkey, err = qsrazureb2c.CheckToken(&mystor, md["token"][0], jwskin)
			if !goodkey {
				if err != nil {
					return status.Errorf(codes.PermissionDenied, " token issue good bye ", err.Error())
				}
			}
		} else {
			return status.Errorf(codes.Unauthenticated, "the token is not supplied, go away")
		}
	}
	log.Println("--> stream interceptor: ", info.FullMethod)
	return handler(srv, stream)

}

func (server *MessageServer) Run(cfg *Configuration, port int32, sslserver bool) error {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	var s *grpc.Server
	if cfg.CertCrt != "" && cfg.CertKey != "" && sslserver {
		tlsCredentials, err := loadTLSCredentials(cfg.CertCrt, cfg.CertKey, cfg.InsecureSkipVerify)
		if err != nil {
			log.Fatal("cannot load TLS credentials: ", err)
		}
		s = grpc.NewServer(grpc.Creds(tlsCredentials),
			grpc.MaxRecvMsgSize(cfg.MaxMsgSize),
			grpc.MaxSendMsgSize(cfg.MaxMsgSize),
			grpc.UnaryInterceptor(unaryInterceptor),
			grpc.StreamInterceptor(streamInterceptor))
	} else {
		s = grpc.NewServer(
			grpc.MaxRecvMsgSize(cfg.MaxMsgSize),
			grpc.MaxSendMsgSize(cfg.MaxMsgSize),
			grpc.UnaryInterceptor(unaryInterceptor),
			grpc.StreamInterceptor(streamInterceptor))
	}
	qsr.RegisterMessageServiceServer(s, server)
	// enable or disable gRPC reflection
	if cfg.GrpcReflection {
		reflection.Register(s)
	}
	log.Printf("server listening at %v", lis.Addr())

	return s.Serve(lis)
}

func createConnection(cfg *Configuration, name string, pg *MessageServer) {
	dburl := cfg.Cloudconnect
	if name == localdb {
		dburl = cfg.Localconnect
	}
	addconn := ""
	if cfg.Maxpool > 0 && cfg.Minpool >= 0 && name != clouddb {
		addconn = addconn + "?pool_max_conns=" + strconv.Itoa(int(cfg.Maxpool)) + "&pool_min_conns=" + strconv.Itoa(int(cfg.Minpool))
	} else {
		addconn = addconn + "&pool_max_conns=" + strconv.Itoa(int(cfg.Maxpool))

	}
	config, err := pgxpool.ParseConfig(dburl + addconn)
	if err != nil {
		log.Printf("Unable to parse config to connection connection:%v %v", name, err)
	}
	config.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		conn.ConnInfo().RegisterDataType(pgtype.DataType{
			Value: &pgtypeuuid.UUID{},
			Name:  "uuid",
			OID:   pgtype.UUIDOID,
		})
		return nil
	}
	dbPool, err := pgxpool.ConnectConfig(context.Background(), config)
	if err != nil {
		log.Printf("Unable to establish connection: %v", err)
	} else {
		log.Printf("the max size of the dpPool %v", dbPool.Config().MaxConns)
		pg.conn[name] = dbPool
	}
}

func startServer(configuration Configuration) {
	enabletokencheck = configuration.EnableTokenCheck
	debugsql = configuration.DebugQuery
	var msg_server *MessageServer = NewMessageServer()
	msg_server.conn = make(map[string]*pgxpool.Pool, 2)
	if configuration.Cloudconnect != "" {
		createConnection(&configuration, clouddb, msg_server)
		defer msg_server.conn[clouddb].Close()
	}
	if configuration.Localconnect != "" {
		createConnection(&configuration, localdb, msg_server)
		defer msg_server.conn[localdb].Close()
	}

	mystor.Keytbl = make(map[string][]interface{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	qcfg := qsrazureb2c.Configuration(configuration)
	jwskin = qsrazureb2c.CreateJWKS(&ctx, &cancel, &qcfg)
	defer jwskin.EndBackground()
	go executeRun(msg_server, configuration, configuration.TLSport, true)
	executeRun(msg_server, configuration, configuration.NonTLSport, false)

}

func executeRun(msg_server *MessageServer, configuration Configuration, port int32, sslserver bool) {
	if err := msg_server.Run(&configuration, port, sslserver); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

func main() {

	file, _ := os.Open("conf.json")
	defer file.Close()
	decoder := json.NewDecoder(file)
	configuration := Configuration{}
	err := decoder.Decode(&configuration)
	if err != nil {
		fmt.Println("error:", err)
	}
	startServer(configuration)

}

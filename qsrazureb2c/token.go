package qsrazureb2c

import (
	"context"
	"log"
	"qsrconnector/qsrkeystore"
	"time"

	"github.com/golang-jwt/jwt/v4"

	"github.com/MicahParks/keyfunc"
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
}

var KeyType = "token"

func CreateJWKS(ctx *context.Context, cancel *context.CancelFunc, cfg *Configuration) *keyfunc.JWKS {
	// Get the JWKS URL.
	jwksURL := cfg.AzureAdInstance + "/" + cfg.AzureAdTenantId + "/" + cfg.AzureAdSignUpSignInPolicyId + "/discovery/v2.0/keys"
	//	jwksURL := "https://qsrulurudev.b2clogin.com/7ec8bc42-ae9c-43c0-b6d3-460b5b996c5f/B2C_1A_SIGNUPORSIGNIN/discovery/v2.0/keys"

	// Create the keyfunc options. Use an error handler that logs. Refresh the JWKS when a JWT signed by an unknown KID
	// is found or at the specified interval. Rate limit these refreshes. Timeout the initial JWKS refresh request after
	// 10 seconds. This timeout is also used to create the initial context.Context for keyfunc.Get.
	options := keyfunc.Options{
		Ctx: *ctx,
		RefreshErrorHandler: func(err error) {
			log.Printf("There was an error with the jwt.Keyfunc\nError: %s", err.Error())
		},
		RefreshInterval:   time.Hour,
		RefreshRateLimit:  time.Minute * 5,
		RefreshTimeout:    time.Second * 10,
		RefreshUnknownKID: true,
	}

	// Create the JWKS from the resource at the given URL.
	jwks, err := keyfunc.Get(jwksURL, options)
	if err != nil {
		log.Fatalf("Failed to create JWKS from resource at the given URL.\nError: %s", err.Error())
	}
	return jwks
}

func GetAccessToken(jwtB64 string, jwkin *keyfunc.JWKS) (bool, error) {
	retval := true

	// Parse the JWT.
	var token *jwt.Token
	var err error

	if token, err = jwt.Parse(jwtB64, jwkin.Keyfunc); err != nil {
		log.Printf("Failed to parse the JWT.\nError: %s", err.Error())
	}

	// Check if the token is valid.
	if !token.Valid {
		log.Printf("the token is not valid.")
		return false, err
	}
	log.Println("The token is valid.")

	return retval, err
}

func CheckToken(kstore *qsrkeystore.KeyMap, jwtB64 string, jwkin *keyfunc.JWKS) (bool, error) {
	var goodstore bool
	var err error
	if !kstore.KeyValueExists(KeyType, jwtB64) {
		goodstore, err = GetAccessToken(jwtB64, jwkin)
		if goodstore {
			kstore.SetkeyValue(KeyType, jwtB64)
			return true, nil
		} else {
			kstore.RemoveKeyValue("token", jwtB64)
		}
	} else {
		return true, err
	}
	return false, err
}

/*
func main() {

	file, _ := os.Open("../qsr_server/conf.json")
	defer file.Close()
	decoder := json.NewDecoder(file)
	configuration := Configuration{}
	err := decoder.Decode(&configuration)
	if err != nil {
		fmt.Println("error:", err)
	}
	mystorage := qsrkeystore.KeyMap{}
	mystorage.Keytbl = make(map[string][]interface{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	jwksin := CreateJWKS(&ctx, &cancel, &configuration)
	defer jwksin.EndBackground()
	for i := 0; i < 1000; i++ {
		jwtB64 := "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsImtpZCI6IjhicE5qV1g2RXlYMjM2VXpSenlGSDI3ZUpfZkZWZHNHN2t6a3ljSGxGUEUifQ.eyJpc3MiOiJodHRwczovL3FzcnVsdXJ1ZGV2LmIyY2xvZ2luLmNvbS83ZWM4YmM0Mi1hZTljLTQzYzAtYjZkMy00NjBiNWI5OTZjNWYvdjIuMC8iLCJleHAiOjE2NTk3NTA2ODAsIm5iZiI6MTY1OTY2NDI4MCwiYXVkIjoiMDRlMmMzN2YtMGZmMC00YTEwLWI1NzQtMzg2MjlmYjNkMDhmIiwic2lnbkluTmFtZSI6Imh6MjAwXzVAeW9wbWFpbC5jb20iLCJzdWIiOiI0ODQyZmExMS01OWUyLTQ0ODEtYTc1Mi03OTZjYjM3MWU3NzciLCJleHRlbnNpb25fYzhkZTBmYTNkOTEyNGFjMDg5YjljMTQyNzJmOGMwMDBfdGVybXNPZlVzZSI6InByaXZhY3lfcG9saWN5LHRlcm1fYW5kX2NvbmRpdGlvbnMiLCJnaXZlbl9uYW1lIjoiTHVhbiIsImZhbWlseV9uYW1lIjoiVHJ1b25nIiwidGlkIjoiN2VjOGJjNDItYWU5Yy00M2MwLWI2ZDMtNDYwYjViOTk2YzVmIiwiaXNGb3Jnb3RQYXNzd29yZCI6ZmFsc2UsIm5vbmNlIjoiMzM2M2NhYjAtODVjMS00NzVmLTk1NWQtY2VjMmM5ZTU5ZjI0Iiwic2NwIjoiYWNjZXNzX2FzX3VzZXIiLCJhenAiOiIzNDUwMWZiOC1kMWU2LTQ5NzMtYWE1Yi05N2IxYTE5NDZjYTkiLCJ2ZXIiOiIxLjAiLCJpYXQiOjE2NTk2NjQyODB9.mrM-w705rfUq6WrJEa0Qre41UYoN_d73julUZjghQd974YgCjJDUz5QSBkseD89goGRXpLBx8_GZXSfOc0ga2rQmEKUiAyY6lFXr1oFj5eTQhHqWu4cIshgn-7lo2IvioxcYZg-Y-G3mtNhMIFQgfsLDAiCQt7vGJYCtNw_yTFu6JmM3MehAF2A1u74hpVk_AcjotLeLd1oB9iZ6xh7yG9lwRYfkE8f_WtJiic2lLwDgd9h8BDK4u1_JhriICKL2BFmkEpURxAke07m_BD-dfdIs8MroSzNA9SWHeXdtZwbNbmJM1wOgEvz7MOugTT9e-5Y8MZsD4xIExahe198Fqg"
		goodkey := CheckToken(&mystorage, jwtB64, jwksin)
		fmt.Println("we have a good key return yes or no:", goodkey)
	}

}
*/

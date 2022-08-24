package qsrkeystore

import "sync"

type KeyMap struct {
	mu     sync.Mutex
	Keytbl map[string][]interface{}
}

func (kt *KeyMap) KeyValueExists(keyid string, keyvalue interface{}) bool {
	kt.mu.Lock()
	list, exists := kt.Keytbl[keyid]
	if exists {
		for i := range list {
			if list[i] == keyvalue {
				kt.mu.Unlock()
				return true
			}
		}
	}
	kt.mu.Unlock()
	return false
}

func (kt *KeyMap) RemoveKeyValue(keyid string, keyvalue interface{}) {
	kt.mu.Lock()
	list, exists := kt.Keytbl[keyid]
	if exists {
		for i := range list {
			if list[i] == keyvalue {
				remove(list, i)
			}
		}
	}
	kt.Keytbl[keyid] = list
	kt.mu.Unlock()
}

func remove(s []interface{}, i int) []interface{} {
	s[i] = s[len(s)-1]
	return s[:len(s)-1]
}

func (kt *KeyMap) SetkeyValue(keyid string, keyvalue interface{}) {
	kt.mu.Lock()
	list, exists := kt.Keytbl[keyid]
	if exists {
		list = append(list, keyvalue)
		kt.Keytbl[keyid] = list
	} else {
		newlist := keyvalue
		kt.Keytbl[keyid] = append(kt.Keytbl[keyid], newlist)
	}
	kt.mu.Unlock()
}

func (kt *KeyMap) GetkeyValues(keyid string) []interface{} {
	return kt.Keytbl[keyid]
}

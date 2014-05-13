package goutil

import (
	"bytes"
	"compress/gzip"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"strings"
	"time"
)

// authentication tokens for the web

const iso = "2006-01-02T15:04:05.000Z"

type Token struct {
	Mac     []byte    `json:"M,omitempty"`
	From    time.Time `json:"F,omitempty"`
	To      time.Time `json:"T,omitempty"`
	Message string    `json:"P,omitempty"`
}

func NewToken(validity time.Duration, message, password string) string {
	buf := new(bytes.Buffer)
	e1 := base64.NewEncoder(base64.URLEncoding, buf)
	e2 := gzip.NewWriter(e1)
	e3 := json.NewEncoder(e2)
	from := time.Now().UTC()
	to := from.Add(validity)
	mac := hmac.New(sha256.New, []byte(password))
	mac.Write([]byte(from.Format(iso)))
	mac.Write([]byte(to.Format(iso)))
	mac.Write([]byte(message))
	m := Token{
		Mac:     mac.Sum(nil),
		From:    from,
		To:      to,
		Message: message,
	}
	e3.Encode(m)
	e2.Close()
	return buf.String()
}

// returns whether or not token is valid, and decoded token
func DecodeToken(tok, password string) (bool, *Token, error) {
	b := base64.NewDecoder(base64.URLEncoding, strings.NewReader(tok))
	gz, err := gzip.NewReader(b)
	if err != nil {
		return false, nil, err
	}
	d := json.NewDecoder(gz)
	var m Token
	err = d.Decode(&m)
	if err != nil {
		return false, nil, err
	}
	mac := hmac.New(sha256.New, []byte(password))
	mac.Write([]byte(m.From.Format(iso)))
	mac.Write([]byte(m.To.Format(iso)))
	mac.Write([]byte(m.Message))
	valid := bytes.Equal(mac.Sum(nil), m.Mac)
	now := time.Now().UTC()
	if now.After(m.To) {
		valid = false
	}
	if m.From.After(now) {
		valid = false
	}
	return valid, &m, nil
}

func encode(m map[string]interface{}) (string, error) {
	buf, err := json.Marshal(m)
	if err != nil {
		return "", err
	}
	return string(buf), nil
}

func decode(s string) (map[string]interface{}, error) {
	m := make(map[string]interface{})
	err := json.Unmarshal([]byte(s), &m)
	return m, err
}

// simple smtp client.
package smtpc

import (
	"bytes"
	"crypto/md5"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/smtp"
	"net/textproto"
	"strings"
)

type Auth struct {
	User     string // for aws ses, the key id
	Password string // for aws ses, the secret key
	Host     string
	Port     int // for aws ses, 587 would work
}

type Meta struct {
	To      []string
	Cc      []string
	From    string
	Subject string
}

type Content struct {
	Type string
	Data []byte
}

type MultipartEmail struct {
	Meta
	Content     []Content // alternative presentations of same content
	Attachments []Attachment
}

type Attachment struct {
	Filename  string
	ContentID string
	Content
}

// calculates a content signature of email
func (m MultipartEmail) Signature() string {
	return m.signContent()[:3] + m.signAttachments()[:3]
}

func (m MultipartEmail) signContent() string {
	x := make(map[string]string)
	x["from"] = m.From
	x["subject"] = m.Subject
	for i, c := range m.Content {
		x[fmt.Sprintf("contenttype-%d", i)] = c.Type
		x[fmt.Sprintf("content-%d", i)] = fmt.Sprintf("%x", c.Data)
	}
	return sign(x)
}

func (m MultipartEmail) signAttachments() string {
	x := make(map[string]int)
	for _, a := range m.Attachments {
		y := make(map[string]interface{})
		y["filename"] = a.Filename
		y["contenttype"] = a.Type
		y["content"] = a.Data
		x[sign(y)]++
	}
	return sign(x)
}

func sign(i interface{}) string {
	buf, err := json.Marshal(i)
	check(err)
	h := md5.New()
	h.Write(buf)
	return fmt.Sprintf("%x", h.Sum(nil))
}

const crlf = "\r\n"

// only allows single content
func SendMulti(auth Auth, email MultipartEmail) error {
	if len(email.Content) != 1 {
		panic("only 1 content supported")
	}
	a := smtp.PlainAuth("", auth.User, auth.Password, auth.Host)
	buf := new(bytes.Buffer)
	boundary := randomBoundary()
	header := make(textproto.MIMEHeader)
	header.Set("Subject", email.Subject)
	header.Set("From", email.From)
	header.Set("To", strings.Join(email.To, ", "))
	if len(email.Cc) > 0 {
		header.Set("Cc", strings.Join(email.Cc, ", "))
	}
	header.Set("MIME-Version", "1.0")
	header.Set("Content-Type", "multipart/mixed; boundary="+boundary)
	for k, v := range header {
		for _, s := range v {
			fmt.Fprintf(buf, "%s: %s%s", k, textproto.TrimString(s), crlf)
		}
	}
	fmt.Fprint(buf, crlf)
	mm := multipart.NewWriter(buf)
	mm.SetBoundary(boundary)
	{
		content := email.Content[0]
		header := make(textproto.MIMEHeader)
		header.Set("Content-Type", content.Type)
		header.Set("Content-Transfer-Encoding", "base64")
		part, err := mm.CreatePart(header)
		if err != nil {
			return err
		}
		lw := &lineWriter{Writer: part, Length: 75}
		e := base64.NewEncoder(base64.StdEncoding, lw)
		e.Write(content.Data)
		e.Close()
	}
	for _, a := range email.Attachments {
		header := make(textproto.MIMEHeader)
		header.Set("Content-Type", fmt.Sprintf(`%s; name="%s"`, a.Type, a.Filename))
		if len(a.ContentID) > 0 {
			header.Set("Content-ID", fmt.Sprintf(`<%s>`, a.ContentID))
		}
		header.Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s"`, a.Filename))
		header.Set("Content-Transfer-Encoding", "base64")
		part, err := mm.CreatePart(header)
		if err != nil {
			return err
		}
		lw := &lineWriter{Writer: part, Length: 75}
		e := base64.NewEncoder(base64.StdEncoding, lw)
		e.Write(a.Data)
		e.Close()
	}
	mm.Close()
	addr := fmt.Sprintf("%s:%d", auth.Host, auth.Port)
	return smtp.SendMail(addr, a, email.From, email.To, buf.Bytes())

}

func randomBoundary() string {
	var buf [30]byte
	_, err := io.ReadFull(rand.Reader, buf[:])
	if err != nil {
		panic(err)
	}
	return fmt.Sprintf("%x", buf[:])
}

// adds linefeeds every Length bytes
type lineWriter struct {
	io.Writer
	Length  int
	current int
}

func (w *lineWriter) Write(p []byte) (int, error) {
	var n int
	for _, b := range p {
		x, err := w.Writer.Write([]byte{b})
		n += x
		w.current += x
		if err != nil {
			return n, err
		}
		if w.current > w.Length {
			_, err := w.Writer.Write([]byte(crlf))
			w.current = 0
			if err != nil {
				return n, err
			}
		}
	}
	return n, nil
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}

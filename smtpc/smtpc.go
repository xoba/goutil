// simple smtp client.
package smtpc

import (
	"bytes"
	"code.google.com/p/go-uuid/uuid"
	"crypto/md5"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"net/smtp"
	"net/textproto"
	"strings"
	"time"
)

type Auth struct {
	User, Password, Host string
	Port                 int
}

type PlainTextEmail struct {
	To      []string
	From    string
	Subject string
	Content string
}

func (p PlainTextEmail) String() string {
	b := new(bytes.Buffer)
	fmt.Fprintf(b, "From: %s\n", p.From)
	fmt.Fprintf(b, "To: %s\n", strings.Join(p.To, ", "))
	fmt.Fprintf(b, "Subject: %s\n", p.Subject)
	fmt.Fprintln(b)
	fmt.Fprint(b, p.Content)
	return string(b.Bytes())
}

type PlainTextMultipartEmail struct {
	PlainTextEmail
	Attachments []Attachment
}

// calculates a content signature of email
func (m PlainTextMultipartEmail) Signature() string {
	return m.signContent()[:3] + m.signAttachments()[:3]
}

type Attachment struct {
	Filename    string
	ContentType string
	Content     []byte
}

func (a Attachment) GetId() string {
	return uuid.New()
}

const crlf = "\r\n"

func Send(auth Auth, email PlainTextEmail) (err error) {

	a := smtp.PlainAuth("", auth.User, auth.Password, auth.Host)

	msg := ""

	msg += "Subject: " + email.Subject + crlf

	for _, x := range email.To {
		msg += "To: " + x + crlf
	}

	msg += crlf

	msg += email.Content

	addr := fmt.Sprintf("%s:%d", auth.Host, auth.Port)

	err = smtp.SendMail(addr, a, email.From, email.To, []byte(msg))
	if err != nil {
		return
	}

	return
}

func SendMultipart(auth Auth, email PlainTextMultipartEmail) error {
	for i, to := range email.To {
		title := fmt.Sprintf("%3d/%3d. %s", i+1, len(email.To), to)
		foreverRetry(title, func() error {
			return sendTo(auth, to, email)
		})
		log.Printf("sent email to %s\n", to)
	}
	return nil
}

func sendTo(auth Auth, to string, email PlainTextMultipartEmail) error {
	a := smtp.PlainAuth("", auth.User, auth.Password, auth.Host)
	buf := new(bytes.Buffer)
	boundary := randomBoundary()
	header := make(textproto.MIMEHeader)
	header.Set("Subject", email.Subject)
	header.Set("From", email.From)
	header.Set("To", to)
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
		header := make(textproto.MIMEHeader)
		header.Set("Content-Type", "text/plain")
		part, err := mm.CreatePart(header)
		if err != nil {
			return err
		}
		fmt.Fprintln(part, email.Content)

	}
	for _, a := range email.Attachments {
		header := make(textproto.MIMEHeader)
		header.Set("Content-Type", fmt.Sprintf(`%s; name="%s"`, a.ContentType, a.Filename))
		header.Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s"`, a.Filename))
		header.Set("Content-Transfer-Encoding", "base64")
		header.Set("X-Attachment-Id", a.GetId())
		part, err := mm.CreatePart(header)
		if err != nil {
			return err
		}
		lw := &lineWriter{Writer: part, Length: 75}
		e := base64.NewEncoder(base64.StdEncoding, lw)
		e.Write(a.Content)
		e.Close()
	}
	mm.Close()
	addr := fmt.Sprintf("%s:%d", auth.Host, auth.Port)
	return smtp.SendMail(addr, a, email.From, []string{to}, buf.Bytes())

}

// calculates a content signature of email
func (m PlainTextMultipartEmail) signContent() string {
	x := make(map[string]string)
	x["from"] = m.From
	x["subject"] = m.Subject
	x["content"] = m.Content
	return sign(x)
}

func sign(i interface{}) string {
	buf, err := json.Marshal(i)
	check(err)
	h := md5.New()
	h.Write(buf)
	return fmt.Sprintf("%x", h.Sum(nil))
}

func (m PlainTextMultipartEmail) signAttachments() string {
	x := make(map[string]int)
	for _, a := range m.Attachments {
		y := make(map[string]interface{})
		y["filename"] = a.Filename
		y["contenttype"] = a.ContentType
		y["content"] = a.Content
		x[sign(y)]++
	}
	return sign(x)
}

func foreverRetry(name string, f func() error) {
	for {
		if err := f(); err == nil {
			return
		} else {
			log.Printf("failed to run %s: %v; going to retry\n", name, err)
			time.Sleep(3 * time.Second)
		}
	}
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

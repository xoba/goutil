// simple smtp client.
package smtpc

import (
	"bytes"
	"fmt"
	"net/smtp"
	"strings"
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
	Auth
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

func Send(email PlainTextEmail) (err error) {

	a := smtp.PlainAuth("", email.User, email.Password, email.Host)

	msg := ""

	msg += "Subject: " + email.Subject + "\n"

	for _, x := range email.To {
		msg += "To: " + x + "\n"
	}

	msg += "\n"

	msg += email.Content

	addr := fmt.Sprintf("%s:%d", email.Host, email.Port)

	err = smtp.SendMail(addr, a, email.From, email.To, []byte(msg))
	if err != nil {
		return
	}

	return
}

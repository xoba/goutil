package smtpc

// plain-text email with no attachments

type PlainTextEmail struct {
	To      []string
	From    string
	Subject string
	Content string
}

func (p PlainTextEmail) ToMultipart() MultipartEmail {
	c := Content{
		Type: "text/plain; charset=utf-8",
		Data: []byte(p.Content),
	}
	return MultipartEmail{
		Meta: Meta{
			To:      p.To,
			From:    p.From,
			Subject: p.Subject,
		},
		Content: []Content{c},
	}
}

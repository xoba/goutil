package aws

type Auth struct {
	Email, Password            string `json:",omitempty"`
	AccessKey, SecretKey       string
	AccountID, CanonicalUserID string `json:",omitempty"`
	Comment                    string `json:",omitempty"`
}

package aws

type Auth struct {
	Email, Password            string
	AccessKey, SecretKey       string
	AccountID, CanonicalUserID string
	Comment                    string
}

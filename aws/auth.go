package aws

type Auth struct {
	Email                      string
	AccessKey, SecretKey       string
	AccountID, CanonicalUserID string
}

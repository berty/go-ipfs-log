package identityprovider

type Interface interface {
	/* GetID Return id of identity (to be signed by orbit-db public key) */
	GetID(id string) (*Identity, error)

	/* SignIdentity Return signature of OrbitDB public key signature */
	SignIdentity(data []byte, id string) ([]byte, error)

	/* GetType Return the type for this identity provider */
	GetType() string
}

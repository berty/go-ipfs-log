package enc

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestNewSecretbox(t *testing.T) {
	boxed1, err := NewSecretbox([]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 1, 2})
	require.NoError(t, err)
	require.NotNil(t, boxed1)

	boxed1, err = NewSecretbox([]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 1})
	require.Error(t, err)
	require.Nil(t, boxed1)

	boxed1, err = NewSecretbox([]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 1, 2, 3})
	require.Error(t, err)
	require.Nil(t, boxed1)

	boxed1, err = NewSecretbox(nil)
	require.Error(t, err)
	require.Nil(t, boxed1)
}

func TestBoxed_Seal_Open(t *testing.T) {
	boxed1, err := NewSecretbox([]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 1, 2})
	require.NoError(t, err)

	boxed2, err := NewSecretbox([]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 1, 3})
	require.NoError(t, err)

	ref1 := []byte("test1")

	sealed1, err := boxed1.Seal(ref1)
	require.NoError(t, err)

	sealed1Other, err := boxed1.Seal(ref1)
	require.NoError(t, err)

	require.NotEqual(t, sealed1, sealed1Other)

	opened1, err := boxed1.Open(sealed1)
	require.NoError(t, err)
	require.Equal(t, ref1, opened1)

	opened2, err := boxed2.Open(sealed1)
	require.Error(t, err)
	require.Nil(t, opened2)

	opened1Other, err := boxed1.Open(sealed1Other)
	require.NoError(t, err)
	require.Equal(t, ref1, opened1Other)

	opened1, err = boxed1.Open(sealed1[0 : len(sealed1)-2])
	require.Error(t, err)
	require.Nil(t, opened1)
}

func TestBoxed_SealWithNonce_OpenWithNonce(t *testing.T) {
	boxed1, err := NewSecretbox([]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 1, 2})
	require.NoError(t, err)

	boxed2, err := NewSecretbox([]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 1, 3})
	require.NoError(t, err)

	ref1 := []byte("test1")
	nonce1 := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3}
	nonce2 := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 4}

	sealed1, err := boxed1.SealWithNonce(ref1, nonce1)
	require.NoError(t, err)

	sealed1Other, err := boxed1.SealWithNonce(ref1, nonce2)
	require.NoError(t, err)
	require.NotEqual(t, ref1, sealed1Other)

	sealed1Other, err = boxed1.SealWithNonce(ref1, nonce1)
	require.NoError(t, err)
	require.Equal(t, sealed1, sealed1Other)

	opened1, err := boxed1.OpenWithNonce(sealed1, nonce1)
	require.NoError(t, err)
	require.Equal(t, ref1, opened1)

	opened2, err := boxed2.OpenWithNonce(sealed1, nonce1)
	require.Error(t, err)
	require.Nil(t, opened2)

	opened2, err = boxed1.OpenWithNonce(sealed1, nonce2)
	require.Error(t, err)
	require.Nil(t, opened2)

	opened1Other, err := boxed1.OpenWithNonce(sealed1Other, nonce1)
	require.NoError(t, err)
	require.Equal(t, ref1, opened1Other)
}

func TestBoxed_DeriveNonce(t *testing.T) {
	boxed1, err := NewSecretbox([]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 1, 2})
	require.NoError(t, err)

	boxed2, err := NewSecretbox([]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 1, 3})
	require.NoError(t, err)

	nonce1A, err := boxed1.DeriveNonce([]byte("A"))
	require.NotEmpty(t, nonce1A)
	require.NoError(t, err)

	nonce1AAgain, err := boxed1.DeriveNonce([]byte("A"))
	require.NotEmpty(t, nonce1AAgain)
	require.NoError(t, err)

	nonce2A, err := boxed2.DeriveNonce([]byte("A"))
	require.NotEmpty(t, nonce2A)
	require.NoError(t, err)

	nonce1B, err := boxed1.DeriveNonce([]byte("B"))
	require.NotEmpty(t, nonce1B)
	require.NoError(t, err)

	require.Equal(t, nonce1A, nonce2A)
	require.NotEqual(t, nonce1A, nonce1B)
	require.Equal(t, nonce1A, nonce1AAgain)
}

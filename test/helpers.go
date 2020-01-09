package test

import "encoding/hex"

func mustBytes(data []byte, err error) []byte {
	if err != nil {
		panic(err)
	}

	return data
}

func mustDecodeHexString(str string) []byte {
	data, err := hex.DecodeString(str)
	if err != nil {
		panic(err)
	}

	return data
}

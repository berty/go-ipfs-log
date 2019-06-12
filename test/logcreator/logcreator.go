package logcreator // import "berty.tech/go-ipfs-log/test/logcreator"

import (
	"fmt"

	idp "berty.tech/go-ipfs-log/identityprovider"
	"berty.tech/go-ipfs-log/io"
	"berty.tech/go-ipfs-log/log"
)

type CreatedLog struct {
	Log          *log.Log
	ExpectedData []string
	JSON         *log.JSONLog
}

func createLogsFor16Entries(ipfs *io.IpfsServices, identities [4]*idp.Identity) (*log.Log, error) {
	logA, err := log.NewLog(ipfs, identities[0], &log.NewLogOptions{ID: "X"})
	if err != nil {
		return nil, err
	}

	logB, err := log.NewLog(ipfs, identities[1], &log.NewLogOptions{ID: "X"})
	if err != nil {
		return nil, err
	}

	log3, err := log.NewLog(ipfs, identities[2], &log.NewLogOptions{ID: "X"})
	if err != nil {
		return nil, err
	}

	l, err := log.NewLog(ipfs, identities[3], &log.NewLogOptions{ID: "X"})
	if err != nil {
		return nil, err
	}

	for i := 1; i <= 5; i++ {
		_, err := logA.Append([]byte(fmt.Sprintf("entryA%d", i)), 1)
		if err != nil {
			return nil, err
		}
	}

	for i := 1; i <= 5; i++ {
		_, err := logB.Append([]byte(fmt.Sprintf("entryB%d", i)), 1)
		if err != nil {
			return nil, err
		}
	}

	_, err = log3.Join(logA, -1)
	if err != nil {
		return nil, err
	}

	_, err = log3.Join(logB, -1)
	if err != nil {
		return nil, err
	}

	for i := 6; i <= 10; i++ {
		_, err := logA.Append([]byte(fmt.Sprintf("entryA%d", i)), 1)
		if err != nil {
			return nil, err
		}
	}

	_, err = l.Join(log3, -1)
	if err != nil {
		return nil, err
	}

	_, err = l.Append([]byte("entryC0"), 1)
	if err != nil {
		return nil, err
	}

	_, err = l.Join(logA, -1)
	if err != nil {
		return nil, err
	}

	return l, nil
}

func CreateLogWithSixteenEntries(ipfs *io.IpfsServices, identities [4]*idp.Identity) (*CreatedLog, error) {
	expectedData := []string{
		"entryA1", "entryB1", "entryA2", "entryB2", "entryA3", "entryB3",
		"entryA4", "entryB4", "entryA5", "entryB5",
		"entryA6",
		"entryC0",
		"entryA7", "entryA8", "entryA9", "entryA10",
	}

	l, err := createLogsFor16Entries(ipfs, identities)
	if err != nil {
		return nil, err
	}

	return &CreatedLog{Log: l, ExpectedData: expectedData, JSON: l.ToJSON()}, nil
}

func createLogWithHundredEntries(ipfs *io.IpfsServices, identities [4]*idp.Identity) (*log.Log, []string, error) {
	var expectedData []string
	const amount = 100

	logA, err := log.NewLog(ipfs, identities[0], &log.NewLogOptions{ID: "X"})
	if err != nil {
		return nil, nil, err
	}

	logB, err := log.NewLog(ipfs, identities[1], &log.NewLogOptions{ID: "X"})
	if err != nil {
		return nil, nil, err
	}

	for i := 1; i <= amount; i++ {
		entryNameA := fmt.Sprintf("entryA%d", i)
		entryNameB := fmt.Sprintf("entryB%d", i)

		_, err := logA.Append([]byte(entryNameA), 1)
		if err != nil {
			return nil, nil, err
		}
		_, err = logB.Join(logA, -1)
		if err != nil {
			return nil, nil, err
		}

		_, err = logB.Append([]byte(entryNameB), 1)
		if err != nil {
			return nil, nil, err
		}

		_, err = logA.Join(logB, -1)
		if err != nil {
			return nil, nil, err
		}

		expectedData = append(expectedData, entryNameA, entryNameB)
	}

	return logA, expectedData, nil
}

func CreateLogWithHundredEntries(ipfs *io.IpfsServices, identities [4]*idp.Identity) (*CreatedLog, error) {
	l, expectedData, err := createLogWithHundredEntries(ipfs, identities)
	if err != nil {
		return nil, err
	}

	return &CreatedLog{Log: l, ExpectedData: expectedData}, nil
}

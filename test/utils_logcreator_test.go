package test

import (
	"context"
	"fmt"

	ipfslog "berty.tech/go-ipfs-log"
	idp "berty.tech/go-ipfs-log/identityprovider"
	core_iface "github.com/ipfs/interface-go-ipfs-core"
)

type CreatedLog struct {
	Log          *ipfslog.IPFSLog
	ExpectedData []string
	JSON         *ipfslog.LogHeads
}

func createLogsFor16Entries(ctx context.Context, ipfs core_iface.CoreAPI, identities []*idp.Identity) (*ipfslog.IPFSLog, error) {
	logA, err := ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: "X"})
	if err != nil {
		return nil, err
	}

	logB, err := ipfslog.NewLog(ipfs, identities[1], &ipfslog.LogOptions{ID: "X"})
	if err != nil {
		return nil, err
	}

	log3, err := ipfslog.NewLog(ipfs, identities[2], &ipfslog.LogOptions{ID: "X"})
	if err != nil {
		return nil, err
	}

	l, err := ipfslog.NewLog(ipfs, identities[3], &ipfslog.LogOptions{ID: "X"})
	if err != nil {
		return nil, err
	}

	for i := 1; i <= 5; i++ {
		_, err := logA.Append(ctx, []byte(fmt.Sprintf("entryA%d", i)), nil)
		if err != nil {
			return nil, err
		}
	}

	for i := 1; i <= 5; i++ {
		_, err := logB.Append(ctx, []byte(fmt.Sprintf("entryB%d", i)), nil)
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
		_, err := logA.Append(ctx, []byte(fmt.Sprintf("entryA%d", i)), nil)
		if err != nil {
			return nil, err
		}
	}

	_, err = l.Join(log3, -1)
	if err != nil {
		return nil, err
	}

	_, err = l.Append(ctx, []byte("entryC0"), nil)
	if err != nil {
		return nil, err
	}

	_, err = l.Join(logA, -1)
	if err != nil {
		return nil, err
	}

	return l, nil
}

func CreateLogWithSixteenEntries(ctx context.Context, ipfs core_iface.CoreAPI, identities []*idp.Identity) (*CreatedLog, error) {
	expectedData := []string{
		"entryA1", "entryB1", "entryA2", "entryB2", "entryA3", "entryB3",
		"entryA4", "entryB4", "entryA5", "entryB5",
		"entryA6",
		"entryC0",
		"entryA7", "entryA8", "entryA9", "entryA10",
	}

	l, err := createLogsFor16Entries(ctx, ipfs, identities)
	if err != nil {
		return nil, err
	}

	return &CreatedLog{Log: l, ExpectedData: expectedData, JSON: l.ToLogHeads()}, nil
}

func createLogWithHundredEntries(ctx context.Context, ipfs core_iface.CoreAPI, identities []*idp.Identity) (*ipfslog.IPFSLog, []string, error) {
	var expectedData []string
	const amount = 100

	logA, err := ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: "X"})
	if err != nil {
		return nil, nil, err
	}

	logB, err := ipfslog.NewLog(ipfs, identities[1], &ipfslog.LogOptions{ID: "X"})
	if err != nil {
		return nil, nil, err
	}

	for i := 1; i <= amount; i++ {
		entryNameA := fmt.Sprintf("entryA%d", i)
		entryNameB := fmt.Sprintf("entryB%d", i)

		_, err := logA.Append(ctx, []byte(entryNameA), nil)
		if err != nil {
			return nil, nil, err
		}
		_, err = logB.Join(logA, -1)
		if err != nil {
			return nil, nil, err
		}

		_, err = logB.Append(ctx, []byte(entryNameB), nil)
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

func CreateLogWithHundredEntries(ctx context.Context, ipfs core_iface.CoreAPI, identities []*idp.Identity) (*CreatedLog, error) {
	l, expectedData, err := createLogWithHundredEntries(ctx, ipfs, identities)
	if err != nil {
		return nil, err
	}

	return &CreatedLog{Log: l, ExpectedData: expectedData}, nil
}

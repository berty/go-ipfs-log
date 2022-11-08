// +build goleak

package test

import (
	"testing"

	"go.uber.org/goleak"
)

// TestDoNothing is used to configure goleak based on leaks due to imports
func TestDoNothing(t *testing.T) {}

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m,
		goleak.IgnoreTopFunction("github.com/ipfs/go-log/writer.(*MirrorWriter).logRoutine"),       // inherited from one of the imports (init)
		goleak.IgnoreTopFunction("go.opencensus.io/stats/view.(*worker).start"),                    // inherited from one of the imports (init)
		goleak.IgnoreTopFunction("github.com/libp2p/go-libp2p-connmgr.(*BasicConnMgr).background"), // inherited from github.com/ipfs/kubo/core.NewNode
		goleak.IgnoreTopFunction("github.com/jbenet/goprocess/periodic.callOnTicker.func1"),        // inherited from github.com/ipfs/kubo/core.NewNode
		goleak.IgnoreTopFunction("github.com/libp2p/go-libp2p-connmgr.(*decayer).process"),         // inherited from github.com/ipfs/kubo/core.NewNode
	)
}

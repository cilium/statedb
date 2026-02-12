package index

import (
	"net/netip"
	"testing"
)

func TestNetIPPrefix(t *testing.T) {
	k1 := NetIPPrefix(netip.MustParsePrefix("10.0.0.100/8"))
	k2 := NetIPPrefix(netip.MustParsePrefix("10.0.0.0/8"))

	if !k1.Equal(k2) {
		t.Errorf("expected netip prefix keys to use canonicalized CIDR")
	}

	k1s, _ := NetIPPrefixString("10.0.0.100/8")
	k2s, _ := NetIPPrefixString("10.0.0.0/8")

	if !k1s.Equal(k2s) {
		t.Errorf("expected netip prefix string keys to use canonicalized CIDR")
	}
}

package statedb

/*
func TestLPM(t *testing.T) {
	lpm := newLPM[int](32)

	cases := []struct {
		addr string
		len  int
	}{
		{"10.1.1.1", 32},    // 0
		{"10.0.0.0", 8},     // 1
		{"192.168.1.0", 24}, // 2
		{"192.168.1.5", 32}, // 3
	}

	for i, c := range cases {
		key := netip.MustParsePrefix(fmt.Sprintf("%s/%d", c.addr, c.len))
		var err error
		lpm, err = lpm.Insert(LPMKey{key.Addr().AsSlice(), key.Bits()}, i)
		require.NoError(t, err)
	}

	lookupAddr := func(addrS string) (int, bool) {
		addr := netip.MustParseAddr(addrS)
		return lpm.Lookup(LPMKey{Key: addr.AsSlice(), PrefixLen: 32})
	}

	for i, c := range cases {
		v, found := lookupAddr(c.addr)
		assert.True(t, found)
		assert.Equal(t, i, v)
	}
	v, found := lookupAddr("10.0.0.1")
	assert.True(t, found)
	assert.Equal(t, 1, v)
	v, found = lookupAddr("192.168.1.4")
	assert.True(t, found)
	assert.Equal(t, 2, v)
	v, found = lookupAddr("192.168.1.6")
	assert.True(t, found)
	assert.Equal(t, 2, v)

	var err error
	lpm, err = lpm.Insert(LPMKey{netip.MustParseAddr("10.1.1.1").AsSlice(), 32}, 999)
	require.NoError(t, err)
	v, found = lookupAddr("10.1.1.1")
	assert.True(t, found)
	assert.Equal(t, 999, v)
}*/

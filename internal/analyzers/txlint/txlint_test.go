// SPDX-License-Identifier: Apache-2.0
// Copyright Authors of Cilium

package txlint_test

import (
	"testing"

	"github.com/cilium/statedb/internal/analyzers/txlint"
	"golang.org/x/tools/go/analysis/analysistest"
)

func TestAnalyzer(t *testing.T) {
	testdata := analysistest.TestData()
	analysistest.Run(t, testdata, txlint.NewAnalyzer(), "a", "nostatedb")
}

func TestAnalyzerStrictSingleLiveTransaction(t *testing.T) {
	testdata := analysistest.TestData()
	analyzer := txlint.NewAnalyzer()
	if err := analyzer.Flags.Set(txlint.StrictSingleLiveTransactionFlag, "true"); err != nil {
		t.Fatal(err)
	}
	analysistest.Run(t, testdata, analyzer, "strict")
}

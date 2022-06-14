package fmtfilter

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCompileFilter(t *testing.T) {
	_, err := CompileFilter("%d", "")
	require.NoError(t, err)
	_, err = CompileFilter("%d", "%s")
	require.Error(t, err, "template ops for scanf don't match scanf ops")
	_, err = CompileFilter("%d%s", "%s")
	require.Error(t, err, "template ops for scanf don't match scanf ops")
	_, err = CompileFilter("dd%d%sdd", "dd%sss")
	require.Error(t, err, "template ops for scanf don't match scanf ops")
	_, err = CompileFilter("dd%d%sdd", "%%")
	require.NoError(t, err)

	_, err = CompileFilter("%", "%s")
	require.Error(t, err, "non-closed %")
	_, err = CompileFilter("%s", "%5")
	require.Error(t, err, "non-closed %")

	_, err = CompileFilter("%f", "%s")
	require.Error(t, err, "unexpected op in position")

	fn, err := CompileFilter("qw%der", "ty%dui")
	require.NoError(t, err)
	_, err = fn("123")
	require.Error(t, err)
	_, err = fn("ty123ui")
	require.Error(t, err)
	_, err = fn("qw123")
	require.Error(t, err)
	_, err = fn("qw123e")
	require.Error(t, err)
	res, err := fn("qw123er")
	require.NoError(t, err)
	require.Equal(t, "ty123ui", res)

	fn, err = CompileFilter("qw%d%2s123%%", "--%d__%s~~%%")
	require.NoError(t, err)
	res, err = fn("qw456AB123")
	require.Error(t, err)
	res, err = fn("qw456AB123%")
	require.NoError(t, err)
	require.Equal(t, "--456__AB~~%", res)

	fn, err = CompileFilter("qw%d%2s123", "--%d__%s~~")
	require.NoError(t, err)
	_, err = fn("qw456ABC123")
	require.Error(t, err)
	res, err = fn("qw456AB123")
	require.NoError(t, err)
	require.Equal(t, "--456__AB~~", res)
}

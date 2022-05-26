package build_info

type BuildInfo struct {
	Version string
	Commit  string
	Branch  string
}

var BuildInfoInstance BuildInfo

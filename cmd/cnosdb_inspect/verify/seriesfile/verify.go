package seriesfile

type Verify struct {
}

func NewVerify() *Verify {
	return &Verify{}
}

func (v Verify) VerifySeriesFile(filePath string) (valid bool, err error) {
	return false, nil
}

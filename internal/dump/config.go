package dump

type Config struct {
	Host                     string
	Port                     int
	Database                 string
	User                     string
	Password                 string
	OutputDir                string
	QueryFile                string
	YBMode                   bool
	EnableBaseScansCostModel bool
	Verbose                  bool
}

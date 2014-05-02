package influxdb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/mozilla-services/heka/message"
	. "github.com/mozilla-services/heka/pipeline"
	hekaTcp "github.com/mozilla-services/heka/plugins/tcp"
)

type InfluxDBOutputConfig struct {
	Server string
	Database string
	Username string
	Password string

	ResponseHeaderTimeout uint `toml:"response_header_timeout"`
	Tls hekaTcp.TlsConfig

	Series string
	UseHekaTimestamp bool `toml:"use_heka_timestamp"`
	FieldMap map[string]string `toml:"field_map"`

	FlushInterval uint `toml:"flush_interval"`
	FlushCount uint `toml:"flush_count"`
}

type InfluxDBOutput struct {
	seriesUrl string
	httpClient *http.Client

	flushInterval time.Duration
	flushCount uint

	series string
	seriesField string
	columns []string
	columnFields []string
}

func (o *InfluxDBOutput) ConfigStruct() interface{} {
	return &InfluxDBOutputConfig{
		Server: "http://localhost:8086",
		Username: "root",
		Password: "root",
		UseHekaTimestamp: true,
		FlushInterval: 1000,
		FlushCount: 10,
		ResponseHeaderTimeout: 30,
	}
}

func (o *InfluxDBOutput) Init(config interface{}) error {
	conf := config.(*InfluxDBOutputConfig)

	serverUrl, err := url.Parse(conf.Server)
	if err != nil {
		return fmt.Errorf("Unable to parse InfluxDB server URL (%s): %s", conf.Server, err)
	}

	switch strings.ToLower(serverUrl.Scheme) {
	case "http", "https":
		serverUrl.Path = path.Join(serverUrl.Path, "db", url.QueryEscape(conf.Database), "series")

		vals := serverUrl.Query()
		vals.Set("time_precision", "u") // microseconds
		if conf.Username != "" {
			vals.Set("u", conf.Username)
		}
		if conf.Password != "" {
			vals.Set("p", conf.Password)
		}
		serverUrl.RawQuery = vals.Encode()
	default:
		return fmt.Errorf("Unable to handle server URL scheme %r", serverUrl.Scheme)
	}
	o.seriesUrl = serverUrl.String()

	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		ResponseHeaderTimeout: time.Duration(conf.ResponseHeaderTimeout) * time.Second,
	}
	if serverUrl.Scheme == "https" {
		tlsConf, err := hekaTcp.CreateGoTlsConfig(&conf.Tls)
		if err != nil {
			return fmt.Errorf("Error settings TLS configuration: %s", err)
		}
		transport.TLSClientConfig = tlsConf
	}
	o.httpClient = &http.Client{Transport: transport}

	if conf.Series == "" && conf.FieldMap["series"] == "" {
		return fmt.Errorf("Must set series or field_map.series")
	}
	o.series = conf.Series

	if conf.UseHekaTimestamp {
		_, hasTime := conf.FieldMap["time"]
		if !hasTime {
			o.columns = []string{"time"}
			o.columnFields = []string{"_hekaTimestampMicro"}
		}
	}

	for column, field := range conf.FieldMap {
		if column == "series" {
			o.seriesField = field
			continue
		}
		o.columns = append(o.columns, column)
		o.columnFields = append(o.columnFields, field)
	}

	o.flushInterval = time.Duration(conf.FlushInterval) * time.Millisecond
	o.flushCount = conf.FlushCount

	return nil
}

type point struct {
	series string
	values []interface{}
}

func (o *InfluxDBOutput) Run(or OutputRunner, h PluginHelper) error {
	chanSize := 10
	flushChan := make(chan []point, chanSize)
	recycleChan := make(chan []point, chanSize)

	go o.flusher(or, flushChan, recycleChan)

	var pack *PipelinePack
	inChan := or.InChan()
	columnCount := len(o.columns)
	msgMax := int(o.flushCount)

	flushTimer := time.NewTimer(0)
	running := true
	for running {
		msgCount := 0

		// Use a recycled []point if available
		var points []point
		select {
		case points = <-recycleChan:
			points = points[:msgMax]
		default:
			points = make([]point, msgMax)
		}

		flushTimer.Reset(o.flushInterval)
	BatchLoop:
		for running {
			select {
			case pack, running = <-inChan:
				if !running {
					break BatchLoop
				}
				pt := &points[msgCount]
				if pt.values == nil {
					pt.values = make([]interface{}, columnCount)
				}
				err := o.process(pt, pack.Message)
				pack.Recycle()

				if err == nil {
					msgCount += 1
				} else {
					or.LogError(err)
				}

				// flush_count
				if msgCount == msgMax {
					break BatchLoop
				}

			case <-flushTimer.C:
				// flush_interval
				if msgCount > 0 {
					break BatchLoop
				} else {
					flushTimer.Reset(o.flushInterval)
				}
			}
		}
		flushChan <-points[:msgCount]
	}
	close(flushChan)

	return nil
}

func (o *InfluxDBOutput) process(pt *point, msg *message.Message) error {
	var series *string
	if o.seriesField != "" {
		series, _ = getField(msg, o.seriesField).(*string)
	}
	if series == nil {
		pt.series = o.series
	} else {
		pt.series = *series
	}
	if pt.series == "" {
		return fmt.Errorf("series_field %s not found", o.seriesField)
	}

	columnCount := len(o.columns)
	for i := 0; i < columnCount; i++ {
		pt.values[i] = getField(msg, o.columnFields[i])
	}

	return nil
}

func (o *InfluxDBOutput) flusher(or OutputRunner, flushChan, recycleChan chan []point) {
	buf := &bytes.Buffer{}
	for pts := range flushChan {
		r, w := io.Pipe()
		go func() {
			buf.Reset()
			jenc := json.NewEncoder(buf)

			buf.Write([]byte{'['})

			var firstPoint bool
			var lastSeries string
			for i, pt := range pts {
				if pt.series != lastSeries {
					if i != 0 {
						buf.Write([]byte(`]},`))
						buf.WriteTo(w)
					}
					buf.Write([]byte(`{"name":`))
					jenc.Encode(pt.series)
					buf.Write([]byte(`,"columns":`))
					jenc.Encode(o.columns)
					buf.Write([]byte(`,"points":[`))
					lastSeries = pt.series
					firstPoint = true
				}
				if firstPoint {
					firstPoint = false
				} else {
					buf.Write([]byte{','})
				}
				jenc.Encode(pt.values)
			}
			buf.Write([]byte(`]}]`))
			buf.WriteTo(w)

			err := w.Close()
			if err != nil {
				or.LogError(err)
			}

			// We're done with pts; release it back to the main loop
			select {
			case recycleChan <- pts:
			default: // No room in the channel; let pts get GCed
			}
		}()

		resp, err := o.httpClient.Post(o.seriesUrl, "application/json", r)
		if err == nil {
			resp.Body.Close()
		} else {
			or.LogError(err)
		}
	}
}

func getField(msg *message.Message, name string) interface{} {
	switch name {
	case "Uuid":
		if msg.Uuid == nil {
			return nil
		}
		return msg.GetUuidString()
	case "Timestamp":
		return msg.Timestamp
	case "Type":
		return msg.Type
	case "Logger":
		return msg.Logger
	case "Severity":
		return msg.Severity
	case "Payload":
		return msg.Payload
	case "EnvVersion":
		return msg.EnvVersion
	case "Pid":
		return msg.Pid
	case "Hostname":
		return msg.Hostname
	case "_hekaTimestampMicro":
		if msg.Timestamp != nil {
			return *msg.Timestamp / 1000 // nano -> micro
		}
		return nil
	default:
		val, _ := msg.GetFieldValue(name)
		return val
	}
}

func init() {
	RegisterPlugin("InfluxDBOutput", func() interface{} {return new(InfluxDBOutput)})
}

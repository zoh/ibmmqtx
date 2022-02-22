package ibmmqtx

import (
	"errors"
	"github.com/sirupsen/logrus"
	"sync"
	"time"
)

const (
	// DefaultReconnectDelay По умолчанию задержка повторных попыткок соединения
	DefaultReconnectDelay = time.Second

	// DefaultPoolSize колво обработчиков в пуле воркеров
	DefaultPoolSize = 10

	DefWaitGetMsgInterval = 3 * 1000

	EncodingUTF8 = 1208
)

var (
	EmptyCorrelId = []byte{23: 0}

	EmptyMsg = Msg{}

	ErrNoEstablishedConnection = errors.New("ibm mq: no established connections")
	ErrNoConnection            = errors.New("ibm mq: no connections")
	ErrNoData                  = errors.New("ibm mq: no data to connect to IBM MQ")
	ErrConnBroken              = errors.New("ibm mq conn: connection broken")
	ErrPutMsg                  = errors.New("ibm mq: failed to put message")
	ErrGetMsg                  = errors.New("ibm mq: failed to get message")
	ErrBrowseMsg               = errors.New("ibm mq: failed to browse message")
	ErrPropsNoField            = errors.New("ibm mq: property is missing")
	errMsgNoField              = "ibm mq: property '%s' is missing"
	errMsgFieldTypeTxt         = "ibm mq: invalid field type '%s'. Got '%T'"
	errMsgFieldType            = errors.New("ibm mq: invalid field type")
)

// Config используется для создания соединения
type Config struct {
	///
	Env

	// подключение / отключение
	mx sync.Mutex

	// повторные подключения при разрыве соединения
	AutoReconnect bool

	// переподключение
	ReconnectDelay time.Duration

	// задаём логгер или будет создан внутри ibmmqtx
	Logger *logrus.Entry
}

type Env struct {
	User             string `env:"MQ_APP_USER"`
	Password         string `env:"MQ_APP_PASSWORD"`
	QManager         string `env:"MQ_QMGR"`
	PutQueueName     string `env:"MQ_PUT_QUEUE"`
	GetQueueName     string `env:"MQ_GET_QUEUE"`
	BrowseQueueName  string `env:"BROWSE_GET_QUEUE"`
	DynamicQueueName string `env:"MQ_DYNAMIC_QUEUE_PREFIX"`
	Host             string `env:"MQ_HOST"`
	Port             string `env:"MQ_PORT"`
	Channel          string `env:"MQ_CHANNEL"`
	Topic            string `env:"MQ_TOPIC_NAME"`
	KeyRepository    string `env:"MQ_KEY_REPOSITORY"`
	TLS              bool   `env:"MQ_TLS"`
	//Cipher           string `env:"MQ_CIPHER"`

	LogLevel logrus.Level `env:"MQ_LOG_LEVEL"`

	// PoolSize pool size for events.
	PoolSize int `env:"MQ_POOL_SIZE"`

	// время ожидания получения сообщений GET (in milliseconds)
	WaitGetMsgInterval int32
}

func (e *Env) Validate() error {
	if e.Host == "" && e.Port == "" {
		return errors.New("empty Host or Port for connection")
	}
	return nil
}

func (e *Env) getConnectionUrl() string {
	return e.Host + "(" + e.Port + ")"
}

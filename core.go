package envdb

import (
	"fmt"
	"os"
	"strconv"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

type DB struct {
	*sqlx.DB `json:"-"`
	Type     string `json:"type"`
	Host     string `json:"host"`
	Port     string `json:"port"`
	Name     string `json:"name"`
	User     string `json:"user"`
	Password string `json:"password"`
	Params   string `json:"params"`
	MaxOpen  int    `json:"maxOpen"`
	MaxIdle  int    `json:"maxIdle"`
	LifeTime int    `json:"lifeTime"`
	IdleTime int    `json:"idleTime"`
}

// ================================================================
//
// ================================================================
func New() (*DB, error) {
	maxOpen, err := strconv.Atoi(os.Getenv("DB_MAX_OPEN"))
	if err != nil {
		return nil, err
	}

	maxIdle, err := strconv.Atoi(os.Getenv("DB_MAX_IDLE"))
	if err != nil {
		return nil, err
	}

	lifeTime, err := strconv.Atoi(os.Getenv("DB_LIFE_TIME"))
	if err != nil {
		return nil, err
	}

	idleTime, err := strconv.Atoi(os.Getenv("DB_IDLE_TIME"))
	if err != nil {
		return nil, err
	}

	return &DB{
		Type:     os.Getenv("DB_TYPE"),
		Host:     os.Getenv("DB_HOST"),
		Port:     os.Getenv("DB_PORT"),
		Name:     os.Getenv("DB_NAME"),
		User:     os.Getenv("DB_USER"),
		Password: os.Getenv("DB_PASSWORD"),
		Params:   os.Getenv("DB_PARAMS"),
		MaxOpen:  maxOpen,
		MaxIdle:  maxIdle,
		LifeTime: lifeTime,
		IdleTime: idleTime,
	}, nil
}

// ================================================================
func (r *DB) Open() error {
	var err error
	r.Close()
	r.DB, err = r.Connect()
	return err
}

func (r *DB) Close() {
	if r.DB != nil {
		r.DB.Close()
	}
}

// ================================================================
//
// ================================================================
func (r DB) Connect() (*sqlx.DB, error) {
	protocol := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?%s", r.User, r.Password, r.Host, r.Port, r.Name, r.Params)

	db, err := sqlx.Open(r.Type, protocol)
	if err != nil {
		return nil, err
	}

	db.SetMaxOpenConns(r.MaxOpen)
	db.SetMaxIdleConns(r.MaxIdle)
	db.SetConnMaxLifetime(time.Duration(r.LifeTime) * time.Second)
	db.SetConnMaxIdleTime(time.Duration(r.IdleTime) * time.Second)

	err = db.Ping()
	if err != nil {
		db.Close()
		return nil, err
	}

	return db, nil
}

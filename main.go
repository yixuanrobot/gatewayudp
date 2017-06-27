// gatewayudp project main.go
package main

import (
	"database/sql"

	"fmt"

	_ "github.com/go-sql-driver/mysql"

	"net"

	"io/ioutil"

	"github.com/bitly/go-simplejson"

	"time"

	"github.com/golang/glog"

	"flag"
)

type Zlog struct {
	LogTime time.Time

	LogType string

	ZoneID int

	PartID int
}

var _ = fmt.Println

var db *sql.DB

func init() {

	flag.Parse()

	filePth := "db.json"

	body, err := ioutil.ReadFile(filePth)

	json, err := simplejson.NewJson(body)

	if err != nil {

		fmt.Println(err)

		glog.Errorf("db.json load failed! err str:%s", err)

		return
	}

	fmt.Printf("%#v", json)

	dbhost, _ := json.Get("dbhost").String()

	dbport, _ := json.Get("dbport").String()

	dbname, _ := json.Get("dbname").String()

	dbuser, _ := json.Get("dbuser").String()

	dbpasword, _ := json.Get("dbpassword").String()

	dbconstr := dbuser + ":"

	dbconstr += dbpasword

	dbconstr += "@tcp("

	dbconstr += dbhost

	dbconstr += ":"

	dbconstr += dbport

	dbconstr += ")/"

	dbconstr += dbname

	dbconstr += "?charset=utf8"

	db, err = sql.Open("mysql", dbconstr)

	if err != nil {

		fmt.Println(err)

		glog.Errorf("database connect failed! err str:%s", err)

		return
	}

	db.SetMaxOpenConns(2000)

	db.SetMaxIdleConns(1000)

	db.Ping()

	glog.Infof("database connect ok! connect str:%s", dbconstr)
}

func main() {

	service := "127.0.0.1:9091"

	addr, err := net.ResolveUDPAddr("udp", service)

	if err != nil {

		fmt.Println(err)

		glog.Errorf("resolve udp %s faild!", service)

		return
	}

	glog.Infof("resolve udp %s success!", service)
	//listener, err := net.ListenMulticastUDP("udp", nil, addr)

	listener, err := net.ListenUDP("udp", addr)

	if err != nil {

		fmt.Println(err)

		glog.Errorf("bind udp %s faild!", service)

		return
	}

	glog.Infof("bind udp %s success!", service)

	fmt.Printf("Local: <%s> \n", listener.LocalAddr().String())

	for {

		handleClient(listener)
	}

	glog.Flush()
}

/*
	handleClient(conn *net.UDPConn)
	data recv
*/
func handleClient(conn *net.UDPConn) {

	data := make([]byte, 1024)

	n, remoteaddr, err := conn.ReadFromUDP(data)

	if err != nil {

		fmt.Printf("error during read: %s", err)

		glog.Errorf("error during read: %s", err)

		return
	}

	fmt.Printf("<%s> %s\n", remoteaddr, data[:n])

	go clientProcess(conn, remoteaddr)
}

func clientProcess(conn *net.UDPConn, addr *net.UDPAddr) {

	rows, err := db.Query("SELECT * FROM user limit 1")

	defer rows.Close()

	checkErr(err)

	columns, _ := rows.Columns()

	scanArgs := make([]interface{}, len(columns))

	values := make([]interface{}, len(columns))

	for j := range values {

		scanArgs[j] = &values[j]

	}

	record := make(map[string]string)

	for rows.Next() {

		//将行数据保存到record字典

		err = rows.Scan(scanArgs...)

		for i, col := range values {

			if col != nil {

				record[columns[i]] = string(col.([]byte))

			}

		}

	}

	fmt.Println(record)

	daytime := time.Now().String()

	conn.WriteToUDP([]byte(daytime), addr)
}

/*
	checkError(err error)
	错误处理
*/
func checkErr(err error) {

	if err != nil {

		fmt.Println(err)

		panic(err)

	}
}

package main

import (
	"database/sql"
	"flag"
	"log"
	"os"
	"time"

	"github.com/cloudspannerecosystem/harbourbridge/conversion"
	"github.com/cloudspannerecosystem/harbourbridge/performance/util"
)

const (
	dbName = "testdb"
)

var (
	recordCount     int
	multipleTableDb bool
)

func main() {
	flag.IntVar(&recordCount, "record-count", 10000, "record-count: Number of rows to add")
	flag.BoolVar(&multipleTableDb, "multiple-table-db", false, "multiple-table-db: it is set to true for populating multiple table database")
	flag.Parse()
	host, user, password, port := os.Getenv("MYSQLHOST"), os.Getenv("MYSQLUSER"), os.Getenv("MYSQLPWD"), os.Getenv("MYSQLPORT")
	connString := conversion.GetMYSQLConnectionStr(host, port, user, password, "")
	db, err := sql.Open("mysql", connString)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	log.Println(time.Now())
	_, err = db.Exec("CREATE DATABASE IF NOT EXISTS " + dbName)
	if err != nil {
		panic(err)
	}

	_, err = db.Exec("USE " + dbName)
	if err != nil {
		panic(err)
	}

	if !multipleTableDb {
		_, err = db.Exec(`CREATE TABLE IF NOT EXISTS employee(employee_id varchar(50), first_name varchar(50) NOT NULL, 
		last_name varchar(50), address varchar(100), dob DATE NOT NULL, is_manager bool NOT NULL, height_in_cm float(4,1) NOT NULL, 
		salary integer NOT NULL, last_updated_time TIMESTAMP NOT NULL)`)
		if err != nil {
			panic(err)
		}
		connString = conversion.GetMYSQLConnectionStr(host, port, user, password, "testdb")
		db, err = sql.Open("mysql", connString)
		if err != nil {
			panic(err)
		}
		qry := `INSERT INTO employee(employee_id, first_name, last_name, address, dob, is_manager, height_in_cm,salary, 
			last_updated_time) VALUES (?,?,?,?,?,?,?,?,?)`
		tx, stmt, e := PrepareTx(db, qry)
		if e != nil {
			panic(e)
		}

		defer tx.Rollback()
		for i := 1; i <= recordCount; i++ {
			_, err = stmt.Exec(util.RandomString(5), util.RandomString(10), util.RandomString(10), util.RandomString(50), util.RandomDate(),
				util.RandomBool(), util.RandomFloat(150, 200), util.RandomInt(1000, 100000), util.CurrentTimestamp())
			if err != nil {
				panic(err)
			}
			// To avoid huge transactions
			if i%50000 == 0 {
				if e := tx.Commit(); e != nil {
					panic(e)
				} else {
					// can only commit once per transaction
					tx, stmt, e = PrepareTx(db, qry)
					if e != nil {
						panic(e)
					}
				}
			}
		}

		// Handle left overs - should also check it isn't already committed
		if e := tx.Commit(); e != nil {
			panic(e)
		}
		log.Println(time.Now())
	}
}

func PrepareTx(db *sql.DB, qry string) (tx *sql.Tx, s *sql.Stmt, e error) {
	if tx, e = db.Begin(); e != nil {
		return
	}

	if s, e = tx.Prepare(qry); e != nil {
		tx.Commit()
	}
	return
}

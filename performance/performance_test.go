// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package performance_test

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/cloudspannerecosystem/harbourbridge/common/constants"
	"github.com/cloudspannerecosystem/harbourbridge/conversion"
	"github.com/cloudspannerecosystem/harbourbridge/performance/util"
	"github.com/cloudspannerecosystem/harbourbridge/testing/common"
	"google.golang.org/appengine/log"
)

// demo struct holds information needed to run the various demo functions.
type demo struct {
	client     *storage.Client
	bucketName string
	bucket     *storage.BucketHandle

	w   io.Writer
	ctx context.Context
	// cleanUp is a list of filenames that need cleaning up at the end of the demo.
	cleanUp []string
	// failed indicates that one or more of the demo steps failed.
	failed bool
}

const (
	dbName                        = "testdb"
	instanceID                    = "shreya-test"
	projectID                     = "e2e-debugging"
	host, user, db_name, password = "localhost", "root", "testdb", "me@SHREYA1998!"
)

func TestMain(m *testing.M) {

	//ctx, done, _ := aetest.NewContext()
	//defer done()
	/*bucket := "shreya-test"

	client, err := storage.NewClient(ctx)
	if err != nil {
		log.Errorf(ctx, "failed to create client: %v", err)
		return
	}
	defer client.Close()

	buf := &bytes.Buffer{}

	d := &demo{
		w:          buf,
		ctx:        ctx,
		client:     client,
		bucket:     client.Bucket(bucket),
		bucketName: bucket,
	}

	n := "demo-testfile-go"
	d.createFile(n)*/
	res := m.Run()
	os.Exit(res)
}

func (d *demo) errorf(format string, args ...interface{}) {
	d.failed = true
	fmt.Fprintln(d.w, fmt.Sprintf(format, args...))
	log.Errorf(d.ctx, format, args...)
}

//[START write]
// createFile creates a file in Google Cloud Storage.
func (d *demo) createFile(fileName string) {
	fmt.Fprintf(d.w, "Creating file /%v/%v\n", d.bucketName, fileName)

	wc := d.bucket.Object(fileName).NewWriter(d.ctx)
	wc.ContentType = "text/plain"
	wc.Metadata = map[string]string{
		"x-goog-meta-foo": "foo",
		"x-goog-meta-bar": "bar",
	}
	d.cleanUp = append(d.cleanUp, fileName)

	if _, err := wc.Write([]byte("abcde\n")); err != nil {
		d.errorf("createFile: unable to write data to bucket %q, file %q: %v", d.bucketName, fileName, err)
		return
	}
	if _, err := wc.Write([]byte(strings.Repeat("f", 1024*4) + "\n")); err != nil {
		d.errorf("createFile: unable to write data to bucket %q, file %q: %v", d.bucketName, fileName, err)
		return
	}
	if err := wc.Close(); err != nil {
		d.errorf("createFile: unable to close bucket %q, file %q: %v", d.bucketName, fileName, err)
		return
	}
}

func TestIntegration_MYSQL_SchemaAndDataSubcommand(t *testing.T) {

	envVars := common.ClearEnvVariables([]string{"MYSQLHOST", "MYSQLUSER", "MYSQLDATABASE", "MYSQLPWD"})
	args := fmt.Sprintf("schema-and-data -source=%s  -source-profile='host=%s,user=%s,db_name=%s,password=%s' -target-profile='instance=%s,dbname=%s'", constants.MYSQL, host, user, db_name, password, instanceID, dbName)
	err := common.RunCommand(args, projectID)
	common.RestoreEnvVariables(envVars)
	if err != nil {
		t.Fatal(err)
	}
}

func TestIntegration_MYSQLDUMP_Command(t *testing.T) {

	dbName := "shreya123"
	dataFilepath := "https://storage.cloud.google.com/shreya-test/mysqldump.test.out"
	filePrefix := "abc.txt"

	args := fmt.Sprintf("-driver %s -prefix %s -instance %s -dbname %s < %s", constants.MYSQLDUMP, filePrefix, instanceID, dbName, dataFilepath)
	err := common.RunCommand(args, projectID)
	if err != nil {
		t.Fatal(err)
	}
}

func TestIntegration_MYSQL_LoadSampleData(t *testing.T) {
	host, user, password, port := os.Getenv("MYSQLHOST"), os.Getenv("MYSQLUSER"), os.Getenv("MYSQLPWD"), os.Getenv("MYSQLPORT")
	connString := conversion.GetMYSQLConnectionStr(host, port, user, password, "")
	db, err := sql.Open("mysql", connString)
	if err != nil {
		print("error")
		return
	}
	defer db.Close()

	db.Exec("DROP DATABASE IF EXISTS " + dbName)
	_, err = db.Exec("CREATE DATABASE " + dbName)
	if err != nil {
		panic(err)
	}

	_, err = db.Exec("USE " + dbName)
	if err != nil {
		panic(err)
	}

	_, err = db.Exec("CREATE TABLE example ( id integer, data varchar(32) )")
	if err != nil {
		panic(err)
	}
	connString = conversion.GetMYSQLConnectionStr(host, port, user, password, "testdb")
	db, err = sql.Open("mysql", connString)
	if err != nil {
		print("error")
		return
	}

	qry := "INSERT INTO example(id, data) VALUES (?, ?)"
	tx, stmt, e := PrepareTx(db, qry)
	if e != nil {
		panic(e)
	}

	defer tx.Rollback()
	for i := 1; i <= 100; i++ {
		_, err = stmt.Exec(util.RandomInt(0, 100), util.RandomString(5))
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

	print("Inserted")

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

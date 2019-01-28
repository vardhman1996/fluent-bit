//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//

package pulsar

import (
	"testing"
	"runtime"
	"net/http"
	"log"
	"encoding/json"
	"bytes"
)

func assertNil(t *testing.T, a interface{}) {
	if a != nil {
		_, file, line, _ := runtime.Caller(1)
		t.Fatalf("%s:%d  | Expected nil", file, line)
	}
}

func assertNotNil(t *testing.T, a interface{}) {
	if a == nil {
		_, file, line, _ := runtime.Caller(1)
		t.Fatalf("%s:%d  | Expected not nil", file, line)
	}
}

func assertEqual(t *testing.T, realValue interface{}, expected interface{}) {
	if realValue != expected {
		_, file, line, _ := runtime.Caller(1)
		t.Fatalf("%s:%d  | Expected '%v' -- Got '%v'", file, line, expected, realValue)
	}
}

func httpPut(url string, body interface{}) {
	client := http.Client{}

	data, _ := json.Marshal(body)
	req, err := http.NewRequest(http.MethodPut, url, bytes.NewReader(data))
	if err != nil {
		log.Fatal(err)
	}

	req.Header = map[string][]string{
		"Content-Type": {"application/json"},
	}

	_, err = client.Do(req)
	if err != nil {
		log.Fatal(err)
	}
}

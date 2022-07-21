// Copyright 2022 Google LLC
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

package webv2

import (
	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
)

const (
	typeString          = "String"
	typeBool            = "Bool"
	typeNumber          = "Number"
	typeNumberString    = "NumberString"
	typeBinary          = "Binary"
	typeList            = "List"
	typeMap             = "Map"
	typeStringSet       = "StringSet"
	typeNumberSet       = "NumberSet"
	typeNumberStringSet = "NumberStringSet"
	typeBinarySet       = "BinarySet"
)

// toSpannerTypeDynamoDB defines the mapping of source types into Spanner
// types. Each source type has a default Spanner type, as well as other potential
// Spanner types it could map to. When calling toSpannerTypeDynamoDB, you specify
// the source type name (along with any modifiers), and optionally you specify
// a target Spanner type name (empty string if you don't have one). If the target
// Spanner type name is specified and is a potential mapping for this source type,
// then it will be used to build the returned ddl.Type. If not, the default
// Spanner type for this source type will be used.
// Note that toSpannerTypeDynamoDB is extensively tested via tests in web_test.go.
//
// TODO: Move the type remapping function to toddl.go (once we've merged
// dynamodb/toddl.go, mysql/toddl.go and postgres/toddl.go).
// Consider some refactoring to reduce code duplication (although note
// that this type remapping has to preserve all previous changes done via the UI!)
func toSpannerTypeDynamoDB(srcType string) (ddl.Type, []internal.SchemaIssue) {
	switch srcType {
	case typeNumber:
		return ddl.Type{Name: ddl.Numeric}, nil
	case typeNumberString, typeString, typeList, typeMap:
		return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, nil
	case typeBool:
		return ddl.Type{Name: ddl.Bool}, nil
	case typeBinary:
		return ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}, nil
	case typeStringSet, typeNumberStringSet:
		return ddl.Type{Name: ddl.String, Len: ddl.MaxLength, IsArray: true}, nil
	case typeNumberSet:
		return ddl.Type{Name: ddl.Numeric, IsArray: true}, nil
	case typeBinarySet:
		return ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength, IsArray: true}, nil
	default:
		return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.NoGoodType}
	}
}

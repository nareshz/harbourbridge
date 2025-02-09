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

package web

import (
	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
)

// toSpannerTypeSQLserver defines the mapping of source types into Spanner
// types. Each source type has a default Spanner type, as well as other potential
// Spanner types it could map to. When calling toSpannerTypeSQLserver, you specify
// the source type name (along with any modifiers), and optionally you specify
// a target Spanner type name (empty string if you don't have one). If the target
// Spanner type name is specified and is a potential mapping for this source type,
// then it will be used to build the returned ddl.Type. If not, the default
// Spanner type for this source type will be used.
// Note that toSpannerTypeSQLserver is extensively tested via tests in web_test.go.
//
// TODO: Move the type remapping function to toddl.go (once we've merged
// dynamodb/toddl.go, mysql/toddl.go, sqlserver/toddl.go and postgres/toddl.go).
// Consider some refactoring to reduce code duplication (although note
// that this type remapping has to preserve all previous changes done via the UI!)
func toSpannerTypeSQLserver(srcType string, spType string, mods []int64) (ddl.Type, []internal.SchemaIssue) {
	switch srcType {
	case "tinyint", "smallint", "int", "bigint":
		switch spType {
		case ddl.String:
			return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.Widened}
		case ddl.Int64:
			return ddl.Type{Name: ddl.Int64}, []internal.SchemaIssue{internal.Widened}
		default:
			return ddl.Type{Name: ddl.Int64}, []internal.SchemaIssue{internal.Widened}
		}
	case "float", "real":
		switch spType {
		case ddl.String:
			return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.Widened}
		default:
			return ddl.Type{Name: ddl.Float64}, []internal.SchemaIssue{internal.Widened}
		}
	case "numeric", "decimal", "money", "smallmoney":
		switch spType {
		case ddl.String:
			return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.Widened}
		default:
			// TODO: check mod[0] and mod[1] and generate a warning
			// if this numeric won't fit in Spanner's NUMERIC.
			return ddl.Type{Name: ddl.Numeric}, nil
		}

	case "bit":
		switch spType {
		case ddl.String:
			return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, nil
		default:
			return ddl.Type{Name: ddl.Bool}, nil
		}
	case "varchar", "char", "nvarchar", "nchar", "uniqueidentifier":
		switch spType {
		case ddl.Bytes:
			if len(mods) > 0 && mods[0] > 0 {
				return ddl.Type{Name: ddl.Bytes, Len: mods[0]}, nil
			}
			return ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}, nil
		default:
			if len(mods) > 0 && mods[0] > 0 {
				return ddl.Type{Name: ddl.String, Len: mods[0]}, nil
			}
			return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, nil
		}
	case "ntext", "text", "xml":
		switch spType {
		case ddl.Bytes:
			return ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}, nil
		default:
			return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, nil
		}

	case "binary", "varbinary", "image":
		switch spType {
		case ddl.String:
			return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, nil
		default:
			return ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}, nil
		}
	case "date":
		switch spType {
		case ddl.String:
			return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.Widened}
		default:
			return ddl.Type{Name: ddl.Date}, nil
		}
	case "datetime2", "datetime", "datetimeoffset", "smalldatetime", "rowversion":
		switch spType {
		case ddl.String:
			return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.Widened}
		default:
			return ddl.Type{Name: ddl.Timestamp}, []internal.SchemaIssue{internal.Datetime}
		}
	case "timestamp":
		switch spType {
		case ddl.String:
			return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.Widened}
		default:
			return ddl.Type{Name: ddl.Int64}, nil
		}
	case "time":
		return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.Time}
	}
	return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.NoGoodType}
}

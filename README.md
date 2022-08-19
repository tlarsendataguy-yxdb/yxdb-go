## yxdb-go

yxdb is a package for reading YXDB files into Go applications. Install it using:

`go get github.com/tlarsendataguy-yxdb/yxdb-go`

The library does not have external dependencies.

The public API is contained in the YxdbReader interface. Instantiate a YxdbReader using one of the two functions:
* `ReadFile(String)` - load from a file
* `ReadStream(io.ReadCloser)` - load from a reader

Iterate through the records in the file using the `Next()` method in a for loop:

```
for reader.Next() {
    // do something
}
```

Fields can be access via the `ReadXxxWithName()` and `ReadXxxWithIndex()` methods on YxdbReader. There are readers for each kind of data field supported by YXDB files:
* `ReadByteWithX()` - read Byte fields
* `ReadBlobWithX()` - read Blob and SpatialObj fields
* `ReadBooleanWithX()` - read Bool fields
* `ReadTimeWithX()` - read Date and DateTime fields
* `ReadFloat64WithX()` - read FixedDecimal, Float, and Double fields
* `ReadInt64WithX()` - read Int16, Int32, and Int64 fields
* `ReadStringWithX()` - read String, WString, V_String, and V_WString fields

The `WithName()` methods read a field by its name. The `WithIndex()` methods read a field by its index in the file.

If either the index number or field name is invalid, the application will panic.
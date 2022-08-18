package meta_info_field

type MetaInfoField struct {
	Name  string `xml:"name,attr"`
	Type  string `xml:"type,attr"`
	Size  int    `xml:"size,attr"`
	Scale int    `xml:"scale,attr"`
}

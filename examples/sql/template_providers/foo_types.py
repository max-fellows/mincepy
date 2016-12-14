def getStrings(db):
	foos = getFoos(db)
	
	stringSubs = {
		"foo_pivot_select": ", ".join(("{0}.bar AS {0}".format(foo) for foo in foos)),
        "foo_pivot_from": ", ".join(("mdb_test_1 {}".format(foo) for foo in foos)),
        "foo_pivot_where": " AND ".join(("{0}.foo LIKE '{0}'".format(foo) for foo in foos))
	}
	
	return stringSubs

	
def getFoos(db):
	sql = "SELECT DISTINCT foo FROM mdb_test_1"
	results = db.query(sql)
	foos = [row[0] for row in results]

	return foos

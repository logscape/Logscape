package com.liquidlabs.log.jreport;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.liquidlabs.log.fields.FieldSet;
import com.liquidlabs.log.fields.field.FieldI;
import com.liquidlabs.orm.Id;

public class JReport {
	
	enum SCHEMA { chartOnly, description, name, output, searchName, department, owner, logo, landscape, visibleFields, mode, printDetails  };

	public String searchName;
	
	
	@Id
	public String name;
	public String description;
	public String output;
	public boolean chartOnly;
	// added
	public String owner;
	public String department;
	public String logo;
	
	// default to portrait
	public boolean landscape;
	// whether or not to print search details/rows
	public boolean printDetails;
	// 0 = search landscape, 1 = search portrain, 2 = dashboard portrait
	public int mode;
	
	// comma delimited list - if empty the default back to fieldSet.visible fields
	public String visibleFields;

	public JReport() {
	}
	public JReport(String title, String description, String searchName, String owner, String department, String logo, boolean landscape, String visibleFields, int mode, boolean printDetails) {
		this.name = title;
		this.description = description;
		this.searchName = searchName;
		this.owner = owner;
		this.department = department;
		this.logo = logo;
		this.landscape = landscape;
		this.visibleFields = visibleFields;
		this.mode = mode;
		this.printDetails = printDetails;
	}

	public JReport(HashMap report) {
		this.name = (String) report.get("name");
		this.searchName = (String) report.get("searchName");
		this.description = (String) report.get("description");
		this.chartOnly = (Boolean) report.get("chartOnly");
		this.department = (String) report.get("dept");
		this.landscape = (Boolean) report.get("landscape");
		this.visibleFields = (String) report.get("visibleFields");
		this.mode = (Integer) report.get("mode");
		this.printDetails = (Boolean) report.get("printDetails");
	}
	public String searches() {
		return searchName;
	}

	public String name() {
		return name;
	}

	public String getSubtitle() {
		if (description == null) return "Source:" + searchName;
		return description;
	}
	public void populateVisibleFields(FieldSet fieldSet) {
		if (fieldSet == null) {
			this.visibleFields = "";
			return;
		}
		boolean popuplateVisibleFields = this.visibleFields == null || this.visibleFields.length() == 0;
		if (!popuplateVisibleFields) return;
		this.visibleFields = "";
		List<FieldI> fields = fieldSet.fields();
		for (FieldI field : fields) {
			if (popuplateVisibleFields && field.isVisible()) {
				if (visibleFields.length() >0) visibleFields += ",";
				visibleFields += field.name();
			}
		}
	}
	public List<String> getVisibleFields(FieldSet fieldSet) {
		if (this.visibleFields == null || this.visibleFields.length() == 0) {
			populateVisibleFields(fieldSet);
		}
		String[] split = this.visibleFields.split(",");
		ArrayList<String> result = new ArrayList<String>();
		for (String fieldName : split) {
			result.add(fieldName);
		}
		return result;
	}
	@Override
	public String toString() {
		return super.toString() + " name:" + this.name + " mode:" + this.mode + " out:" + this.output + " vis:" + this.visibleFields;
	}
}

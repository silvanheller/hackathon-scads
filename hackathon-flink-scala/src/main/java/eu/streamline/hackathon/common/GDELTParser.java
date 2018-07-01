package eu.streamline.hackathon.common;

import eu.streamline.hackathon.common.data.GDELTEvent;

import java.io.Serializable;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

public class GDELTParser implements Serializable {

	private static final String DELIMITER = "\t";
    private static final DateFormat DF_GDELT = new SimpleDateFormat("yyyyMMdd");

    private ArrayList<Field> fields;


    private ArrayList<Field> generateGeo() {
    	ArrayList<Field> geo = new ArrayList<Field>();
		geo.add(new Field("type", Integer.class));
		geo.add(new Field("name", String.class));
		geo.add(new Field("countryCode", String.class));
		geo.add(new Field("adm1Code", String.class));
		geo.add(new Field("lat", Double.class));
		geo.add(new Field("long", Double.class));
		geo.add(new Field("featureId", Integer.class));
		return geo;
	}

	 private ArrayList<Field> generateCodes() {
		ArrayList<Field> codes = new ArrayList<>();
		codes.add(new Field("code", String.class));
		codes.add(new Field("name", String.class));
		codes.add(new Field("countryCode", String.class));
		codes.add(new Field("knownGroupCode", String.class));
		codes.add(new Field("ethnicCode", String.class));
		codes.add(new Field("religion1Code", String.class));
		codes.add(new Field("religion2Code", String.class));
		codes.add(new Field("type1Code", String.class));
		codes.add(new Field("type2Code", String.class));
		codes.add(new Field("type3Code", String.class));
		return codes;
	}


	public GDELTParser() {

		fields = new ArrayList<>();

		fields.add(new Field("globalEventID", Integer.class));
		fields.add(new Field("day", Date.class));
		fields.add(new Field("month", Integer.class));
		fields.add(new Field("year", Integer.class));
		fields.add(new Field("fracDate", Double.class));
		fields.add(new Field("actor1Code", generateCodes()));
		fields.add(new Field("actor2Code", generateCodes()));
		fields.add(new Field("isRoot", Boolean.class));
		fields.add(new Field("eventCode", String.class));
		fields.add(new Field("eventBaseCode", String.class));
		fields.add(new Field("eventRootCode", String.class));
		fields.add(new Field("quadClass", Integer.class));
		fields.add(new Field("goldstein", Double.class));
		fields.add(new Field("numMentions", Integer.class));
		fields.add(new Field("numSources", Integer.class));
		fields.add(new Field("numArticles", Integer.class));
		fields.add(new Field("avgTone", Double.class));
		fields.add(new Field("actor1Geo", generateGeo()));
		fields.add(new Field("actor2Geo", generateGeo()));
		fields.add(new Field("eventGeo", generateGeo()));
		fields.add(new Field("dateAdded", Date.class));
		fields.add(new Field("sourceUrl", String.class));

	}

	public GDELTEvent readRecord(String input) throws Exception {

		String[] tokens = input.split(DELIMITER);

		for(int i = 0, j = 0 ; i < tokens.length; j++){
			Field curr = fields.get(j);
			if (curr.hasChildren()) {
				for (Field child : curr.getChildren()) {
					child.setValue(tokens[i++]);
				}
			} else {
				curr.setValue(tokens[i++]);
			}
		}

		GDELTEvent event = new GDELTEvent();
		Class<?> clz = event.getClass();

		try {
			for (Field field : fields) {
				if (field.hasChildren()) {
					for (Field child : field.getChildren()) {
						clz.getField(field.name + "_" + child.name).set(event, child.value);
					}
				} else {
					clz.getField(field.name).set(event, field.value);
				}
			}
		} catch (Exception e) {
			throw e;
		}

		return event;
	}


	
	private class Field implements Serializable {

		private String name;
		private Class<?> clazz;
		private Object value;
		private ArrayList<Field> children;


		public Field(String name, Class<?> clazz) {
			this.name = name;
			this.clazz = clazz;
		}
		
		public Field(String name, ArrayList<Field> children) {
			this.name = name;
			this.children = children;
		}

		public boolean hasChildren() {
			return children != null && children.size() > 0;
		}

		public ArrayList<Field> getChildren() {
			return children;
		}


		public void setValue(String str) {
			if (clazz == null && children.size() == 0){
				throw new IllegalArgumentException("Class not supplied for token " + name);
			}
			if (str == null || str.equals("")) {
				value = null;
				return;
			}
			try {
				if (clazz.equals(Integer.class)) {
					value = Integer.parseInt(str);
				} else if (clazz.equals(Double.class)) {
					value =  Double.parseDouble(str);
				} else if (clazz.equals(Date.class)) {
					value = DF_GDELT.parse(str);
				} else if (clazz.equals(Boolean.class)) {
					value = str.equals("1");
				} else if (clazz.equals(String.class)) {
					value = str;
				} else {
					value = null;
				}
			} catch (Exception ex) {
				value = null;
			}

		}

	}




}

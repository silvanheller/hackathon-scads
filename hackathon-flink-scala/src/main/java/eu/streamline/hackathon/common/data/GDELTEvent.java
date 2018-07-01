package eu.streamline.hackathon.common.data;

import java.io.Serializable;
import java.util.Date;

public class GDELTEvent implements Serializable {

	// this code is automatic generated - DO NOT EDIT IT!

	public Integer globalEventID;
	public Date day;
	public Integer month;
	public Integer year;
	public Double fracDate;
	public Boolean isRoot;
	public String eventCode;
	public String eventBaseCode;
	public String eventRootCode;
	public Integer quadClass;
	public Double goldstein;
	public Integer numMentions;
	public Integer numSources;
	public Integer numArticles;
	public Double avgTone;
	public Date dateAdded;
	public String sourceUrl;
	public String actor1Code_code;
	public String actor2Code_code;
	public String actor1Code_name;
	public String actor2Code_name;
	public String actor1Code_countryCode;
	public String actor2Code_countryCode;
	public String actor1Code_knownGroupCode;
	public String actor2Code_knownGroupCode;
	public String actor1Code_ethnicCode;
	public String actor2Code_ethnicCode;
	public String actor1Code_religion1Code;
	public String actor2Code_religion1Code;
	public String actor1Code_religion2Code;
	public String actor2Code_religion2Code;
	public String actor1Code_type1Code;
	public String actor2Code_type1Code;
	public String actor1Code_type2Code;
	public String actor2Code_type2Code;
	public String actor1Code_type3Code;
	public String actor2Code_type3Code;
	public Integer actor1Geo_type;
	public Integer actor2Geo_type;
	public Integer eventGeo_type;
	public String actor1Geo_name;
	public String actor2Geo_name;
	public String eventGeo_name;
	public String actor1Geo_countryCode;
	public String actor2Geo_countryCode;
	public String eventGeo_countryCode;
	public String actor1Geo_adm1Code;
	public String actor2Geo_adm1Code;
	public String eventGeo_adm1Code;
	public Double actor1Geo_lat;
	public Double actor2Geo_lat;
	public Double eventGeo_lat;
	public Double actor1Geo_long;
	public Double actor2Geo_long;
	public Double eventGeo_long;
	public Integer actor1Geo_featureId;
	public Integer actor2Geo_featureId;
	public Integer eventGeo_featureId;

	@Override
	public String toString() {
		return "GDELTEvent{" +
				"globalEventID=" + globalEventID +
				", day=" + day +
				", month=" + month +
				", year=" + year +
				", fracDate=" + fracDate +
				", isRoot=" + isRoot +
				", eventCode='" + eventCode + '\'' +
				", eventBaseCode='" + eventBaseCode + '\'' +
				", eventRootCode='" + eventRootCode + '\'' +
				", quadClass=" + quadClass +
				", goldstein=" + goldstein +
				", numMentions=" + numMentions +
				", numSources=" + numSources +
				", numArticles=" + numArticles +
				", avgTone=" + avgTone +
				", dateAdded=" + dateAdded +
				", sourceUrl='" + sourceUrl + '\'' +
				", actor1Code_code='" + actor1Code_code + '\'' +
				", actor2Code_code='" + actor2Code_code + '\'' +
				", actor1Code_name='" + actor1Code_name + '\'' +
				", actor2Code_name='" + actor2Code_name + '\'' +
				", actor1Code_countryCode='" + actor1Code_countryCode + '\'' +
				", actor2Code_countryCode='" + actor2Code_countryCode + '\'' +
				", actor1Code_knownGroupCode='" + actor1Code_knownGroupCode + '\'' +
				", actor2Code_knownGroupCode='" + actor2Code_knownGroupCode + '\'' +
				", actor1Code_ethnicCode='" + actor1Code_ethnicCode + '\'' +
				", actor2Code_ethnicCode='" + actor2Code_ethnicCode + '\'' +
				", actor1Code_religion1Code='" + actor1Code_religion1Code + '\'' +
				", actor2Code_religion1Code='" + actor2Code_religion1Code + '\'' +
				", actor1Code_religion2Code='" + actor1Code_religion2Code + '\'' +
				", actor2Code_religion2Code='" + actor2Code_religion2Code + '\'' +
				", actor1Code_type1Code='" + actor1Code_type1Code + '\'' +
				", actor2Code_type1Code='" + actor2Code_type1Code + '\'' +
				", actor1Code_type2Code='" + actor1Code_type2Code + '\'' +
				", actor2Code_type2Code='" + actor2Code_type2Code + '\'' +
				", actor1Code_type3Code='" + actor1Code_type3Code + '\'' +
				", actor2Code_type3Code='" + actor2Code_type3Code + '\'' +
				", actor1Geo_type=" + actor1Geo_type +
				", actor2Geo_type=" + actor2Geo_type +
				", eventGeo_type=" + eventGeo_type +
				", actor1Geo_name='" + actor1Geo_name + '\'' +
				", actor2Geo_name='" + actor2Geo_name + '\'' +
				", eventGeo_name='" + eventGeo_name + '\'' +
				", actor1Geo_countryCode='" + actor1Geo_countryCode + '\'' +
				", actor2Geo_countryCode='" + actor2Geo_countryCode + '\'' +
				", eventGeo_countryCode='" + eventGeo_countryCode + '\'' +
				", actor1Geo_adm1Code='" + actor1Geo_adm1Code + '\'' +
				", actor2Geo_adm1Code='" + actor2Geo_adm1Code + '\'' +
				", eventGeo_adm1Code='" + eventGeo_adm1Code + '\'' +
				", actor1Geo_lat=" + actor1Geo_lat +
				", actor2Geo_lat=" + actor2Geo_lat +
				", eventGeo_lat=" + eventGeo_lat +
				", actor1Geo_long=" + actor1Geo_long +
				", actor2Geo_long=" + actor2Geo_long +
				", eventGeo_long=" + eventGeo_long +
				", actor1Geo_featureId=" + actor1Geo_featureId +
				", actor2Geo_featureId=" + actor2Geo_featureId +
				", eventGeo_featureId=" + eventGeo_featureId +
				'}';
	}
}

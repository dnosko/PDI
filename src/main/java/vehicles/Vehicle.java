package vehicles;


import com.fasterxml.jackson.databind.JsonNode;

import java.text.SimpleDateFormat;
import java.util.Date;


public class Vehicle {
    String id;
    short vtype;
    short ltype;
    double lat;
    double lng;
    double bearing;
    long lineid;
    String linename;
    long routeid;
    String course;
    String lf;
    double delay;
    long laststopid;
    long finalstopid;
    String isinactive;
    Date lastupdate;

        public Vehicle(JsonNode attributes){
            this.id = attributes.path("id").asText();
            this.vtype = (short) attributes.path("vtype").asInt();
            this.ltype = (short) attributes.path("ltype").asInt();
            this.lat =  attributes.path("lat").asDouble();
            this.lng = attributes.path("lng").asDouble();
            this.bearing = attributes.path("bearing").asDouble();
            this.lineid = attributes.path("lineid").asInt();
            this.linename = attributes.path("linename").asText();
            this.routeid = attributes.path("routeid").asInt();
            this.course = attributes.path("course").asText();
            this.lf = attributes.path("lf").asText();
            this.delay = attributes.path("delay").asInt();
            this.laststopid = attributes.path("laststopid").asInt();
            this.finalstopid = attributes.path("finalstopid").asInt();
            this.isinactive = attributes.path("isinactive").asText();
            this.lastupdate = new Date(attributes.path("lastupdate").asLong());
        }

        @Override
        public String toString() {
            return "Vehicle{" +
                    "id='" + this.id + '\'' +
                    ", vtype=" + this.vtype +
                    ", ltype=" + this.ltype +
                    ", lat=" + this.lat +
                    ", lng=" + this.lng +
                    ", bearing=" + this.bearing +
                    ", lineid=" + this.lineid +
                    ", linename='" + this.linename + '\'' +
                    ", routeid=" + this.routeid +
                    ", course='" + this.course + '\'' +
                    ", lf='" + this.lf + '\'' +
                    ", delay=" + this.delay +
                    ", laststopid=" + this.laststopid +
                    ", finalstopid=" + this.finalstopid +
                    ", isinactive='" + this.isinactive + '\'' +
                    ", lastupdate=" + this.lastupdate + '\''+
                    '}';
        }


}



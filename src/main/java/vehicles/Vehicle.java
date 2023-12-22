package vehicles;


import com.fasterxml.jackson.databind.JsonNode;
import lombok.Getter;

import java.util.Date;
import java.util.Objects;


public class Vehicle {
    @Getter
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

    // for constructing test mock data
    public Vehicle(String id, short vtype, double bearing, int lineid, String linename, int delay, int laststopid, Long lastupdate){
        this.id = id;
        this.vtype = vtype;
        this.ltype = vtype;
        this.lat =  0.0;
        this.lng = 0.0;
        this.bearing = bearing;
        this.lineid = lineid;
        this.linename = linename;
        this.routeid = 0;
        this.course = "";
        this.lf = "";
        this.delay = delay;
        this.laststopid = laststopid;
        this.finalstopid = -1;
        this.isinactive = "false";
        this.lastupdate = new Date(lastupdate);
    }

    public double getDelay() {
        return delay;
    }

    public long getLastUpdateLong(){
            return lastupdate.getTime();
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

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        Vehicle vehicle = (Vehicle) obj;

        return vtype == vehicle.vtype &&
                ltype == vehicle.ltype &&
                Double.compare(vehicle.lat, lat) == 0 &&
                Double.compare(vehicle.lng, lng) == 0 &&
                Double.compare(vehicle.bearing, bearing) == 0 &&
                lineid == vehicle.lineid &&
                routeid == vehicle.routeid &&
                Double.compare(vehicle.delay, delay) == 0 &&
                laststopid == vehicle.laststopid &&
                finalstopid == vehicle.finalstopid &&
                id.equals(vehicle.id) &&
                linename.equals(vehicle.linename) &&
                course.equals(vehicle.course) &&
                lf.equals(vehicle.lf) &&
                isinactive.equals(vehicle.isinactive) &&
                lastupdate.equals(vehicle.lastupdate);
    }
}



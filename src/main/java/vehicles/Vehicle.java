package vehicles;


import java.util.Date;

public class Vehicle {

    public Geometry geometry;
    public Attributes attributes;

    public Vehicle(Geometry geometry, Attributes attributes) {
        this.geometry = geometry;
        this.attributes = attributes;
    }

    public String toString() {
        return "Vehicle{" +
                "geometry=" + geometry +
                ", attributes=" + attributes +
                '}';
    }

    class Attributes{
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

        public Attributes(String id, short vtype, short ltype, double lat, double lng, double bearing, long lineid,
                          String linename, long routeid, String course, String lf, double delay, long laststopid,
                          long finalstopid, String isinactive, Date lastupdate){
            this.id = id;
            this.vtype = vtype;
            this.ltype = ltype;
            this.lat = lat;
            this.lng = lng;
            this.bearing = bearing;
            this.lineid = lineid;
            this.linename = linename;
            this.routeid = routeid;
            this.course = course;
            this.lf = lf;
            this.delay = delay;
            this.laststopid = laststopid;
            this.finalstopid = finalstopid;
            this.isinactive = isinactive;
            this.lastupdate = lastupdate;
        }

        @Override
        public String toString() {
            return "Attributes{" +
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
                    ", lastupdate=" + this.lastupdate +
                    '}';
        }
    }

    class Geometry {
        double x;
        double y;
        SpatialReference sr;
        public Geometry(double x, double y, SpatialReference sr){
            this.x = x;
            this.y = y;
            this.sr = sr;
        }
        @Override
        public String toString() {
            return "Geometry{" +
                    "x=" + this.x +
                    ", y=" + this.y +
                    ", sr=" + this.sr +
                    '}';
        }

        class SpatialReference{
            int wkid;

            public SpatialReference(int wkid){
                this.wkid = wkid;
            }
            @Override
            public String toString() {
                return "SpatialReference{" +
                        "wkid=" + this.wkid +
                        '}';
            }
        }
    }
    }

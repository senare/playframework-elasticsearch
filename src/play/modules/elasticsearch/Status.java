package play.modules.elasticsearch;

import play.db.Model;

public class Status {

    private Class<? extends Model> clazz;
    private boolean index;
    private boolean river;

    public Status(Class<? extends Model> clazz, boolean index, boolean river) {
        this.clazz = clazz;
        this.index = index;
        this.river = river;
    }

    public String getClazz() {
        return clazz.getCanonicalName();
    }

    public boolean isIndex() {
        return index;
    }

    public boolean isRiver() {
        return river;
    }

    @Override
    public String toString() {
        return "Status [ clazz=" + clazz + ", index=" + index + ", river=" + river + "]";
    }
}
/**
 * Created by rmkujala on 5/10/16.
 */
public class IntFtcTuple {
    private Integer i;
    private FromToCoordinates ftc;

    public IntFtcTuple(Integer i, FromToCoordinates ftc) {
        this.i = i;
        this.ftc = ftc;
    }

    public Integer getInt() {
        return this.i;
    }

    public FromToCoordinates getFromToCoordinates() {
        return this.ftc;
    }
}

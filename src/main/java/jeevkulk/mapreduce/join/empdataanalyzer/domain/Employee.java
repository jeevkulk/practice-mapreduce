package jeevkulk.mapreduce.join.empdataanalyzer.domain;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Employee implements Writable {
    private String employeeId;
    private String employeeName;
    private String employeeDob;
    private String sex;
    private String maritalStatus;
    private String employeeStatus;

    public String getEmployeeId() {
        return employeeId;
    }

    public void setEmployeeId(String employeeId) {
        this.employeeId = employeeId;
    }

    public String getEmployeeName() {
        return employeeName;
    }

    public void setEmployeeName(String employeeName) {
        this.employeeName = employeeName;
    }

    public String getEmployeeDob() {
        return employeeDob;
    }

    public void setEmployeeDob(String employeeDob) {
        this.employeeDob = employeeDob;
    }

    public String getSex() {
        return sex;
    }

    public void setSex(String sex) {
        this.sex = sex;
    }

    public String getMaritalStatus() {
        return maritalStatus;
    }

    public void setMaritalStatus(String maritalStatus) {
        this.maritalStatus = maritalStatus;
    }

    public String getEmployeeStatus() {
        return employeeStatus;
    }

    public void setEmployeeStatus(String employeeStatus) {
        this.employeeStatus = employeeStatus;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        WritableUtils.writeString(dataOutput, employeeId);
        WritableUtils.writeString(dataOutput, employeeName);
        WritableUtils.writeString(dataOutput, employeeDob);
        WritableUtils.writeString(dataOutput, sex);
        WritableUtils.writeString(dataOutput, maritalStatus);
        WritableUtils.writeString(dataOutput, employeeStatus);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        employeeId = WritableUtils.readString(dataInput);
        employeeName = WritableUtils.readString(dataInput);
        employeeDob = WritableUtils.readString(dataInput);
        sex = WritableUtils.readString(dataInput);
        maritalStatus = WritableUtils.readString(dataInput);
        employeeStatus = WritableUtils.readString(dataInput);
    }

    @Override
    public String toString() {
        return "Employee{" +
                "employeeId='" + employeeId + '\'' +
                ", employeeName='" + employeeName + '\'' +
                ", employeeDob='" + employeeDob + '\'' +
                ", sex='" + sex + '\'' +
                ", maritalStatus='" + maritalStatus + '\'' +
                ", employeeStatus='" + employeeStatus + '\'' +
                '}';
    }
}

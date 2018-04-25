package io.scalecube.services.transport.dispatchers;

public class ErrData {
    int errCode;
    String errMessage;

    public int getErrCode() {
        return errCode;
    }

    public ErrData setErrCode(int errCode) {
        this.errCode = errCode;
        return this;
    }

    public String getErrMessage() {
        return errMessage;
    }

    public ErrData setErrMessage(String errMessage) {
        this.errMessage = errMessage;
        return this;
    }

    @Override
    public String toString() {
        return "ErrData{" +
                "errCode=" + errCode +
                ", errMessage='" + errMessage + '\'' +
                '}';
    }
}

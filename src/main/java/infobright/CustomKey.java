package infobright;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * 二次排序自定义key
 * @author YMY
 *
 */
public class CustomKey implements WritableComparable<CustomKey> {
	// 暂时按照此三个字段排序
	private Long timeStamp;
	private Long appId;
	private Long appVersionId;

	public CustomKey() {	}
	public CustomKey( Long timeStamp, Long appId,Long appVersionId) {	
		this.timeStamp = timeStamp;
		this.appId = appId;
		this.appVersionId = appVersionId;
	}
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(this.timeStamp);
		out.writeLong(this.appId);
		out.writeLong(this.appVersionId);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.timeStamp = in.readLong();
		this.appId = in.readLong();
		this.appVersionId = in.readLong();
	}
	
	@Override
	public int compareTo(CustomKey o) {
		int ts = this.timeStamp.compareTo(o.timeStamp);
		int ai = this.appId.compareTo(o.appId);
		int avi = this.appVersionId.compareTo(o.appVersionId);
		
		if(ts != 0){
			return ts;
		}else{
			if(ai != 0)
				return ai;
			else
				return avi;
		}
	}
	
	@Override
	public String toString(){
		return this.timeStamp+"\t"+this.appId+"\t"+this.appVersionId;
	}

	public Long getTimeStamp() {
		return timeStamp;
	}

	public void setTimeStamp(Long timeStamp) {
		this.timeStamp = timeStamp;
	}

	public Long getAppId() {
		return appId;
	}

	public void setAppId(Long appId) {
		this.appId = appId;
	}

	public Long getAppVersionId() {
		return appVersionId;
	}

	public void setAppVersionId(Long appVersionId) {
		this.appVersionId = appVersionId;
	}
	
}

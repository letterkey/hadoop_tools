package test.pathfilter;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.util.StringUtils;

/**
 * 自定义文件过滤
 * @author YHT
 *
 */
public class CookieFilter implements PathFilter {
	
	@Override
	public boolean accept(Path path) {
		if (StringUtils.split(path.getName(), StringUtils.ESCAPE_CHAR, '-')[1].equals("A")){  
            return true;  
        }
		return false;
	}
}

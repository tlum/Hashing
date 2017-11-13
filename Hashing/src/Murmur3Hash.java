import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import com.google.common.base.Charsets;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

public class Murmur3Hash  extends EvalFunc<String>  {
	
	private HashFunction hf = Hashing.murmur3_32();
	
	public String exec(Tuple input) throws IOException {
		
		HashCode hashCode;
		
		if (input == null || input.size() == 0)
			return null;
		try {
			String str = (String) input.get(0);
			hashCode = hf.newHasher().putString((String)str,Charsets.UTF_8).hash();
			return Integer.toHexString(hashCode.asInt());
		} catch (Exception e) {
			throw new IOException("Caught exception processing input row ", e);
		}
	}
}

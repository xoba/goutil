package jdbc

const javasrc = `
import java.sql.*;
import java.io.*;
import java.util.*;

public class GoJdbc {

    public static void main(String... args) throws Exception {
	try {
	    run(args);
	} catch (Exception e) {
	    e.printStackTrace();
	}
    }

    private static void writeString(DataOutputStream out, String s) throws Exception {
	if (s==null) {
	    s = "";
	}
	byte[] buf = s.getBytes("UTF-8");
	out.writeInt(buf.length);
	out.write(buf);
    }

    private static String readString(DataInputStream in) throws Exception {
	int len = in.readInt();
	byte[] buf = new byte[len];
	in.readFully(buf);
	return new String(buf,"UTF-8");
    }
  
    public static void run(String... args) throws Exception {
	Class.forName(args[0]); // loads the driver

	Connection connection = DriverManager.getConnection(args[1],args[2],args[3]);
	connection.setAutoCommit(true);
	connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);

	Map<String,PreparedStatement> stmts = new HashMap<String,PreparedStatement>();
	Map<String,ResultSet> results = new HashMap<String,ResultSet>();

	DataInputStream dataIn = new DataInputStream(System.in);
	DataOutputStream dataOut = new DataOutputStream(System.out);

writeString(dataOut,"d75b40c8-ee5c-4f64-86e2-2e2e936e7aa6");
dataOut.flush();

	boolean done = false;
	while (!done) {
	    try {
		byte selector = dataIn.readByte();
		switch (selector) {
		case 1: // DONE
		    {
			done = true;
			dataOut.writeByte(1);
			break;
		    }
		case 11: // begin transaction
		    {
			connection.setAutoCommit(false);
			break;
		    }
		case 12: // commit transaction
		    {
			connection.commit();
			connection.setAutoCommit(true);
			break;
		    }
		case 13: // rollback transaction
		    {
			connection.rollback();
			connection.setAutoCommit(true);
			break;
		    }
		case 9: // close statement 
		    {
			String id = readString(dataIn);
			PreparedStatement s = stmts.get(id);
			s.close();
			stmts.remove(id);
			break;
		    }
		case 10: // close result set 
		    {
			String id = readString(dataIn);
			ResultSet rs = results.get(id);
			rs.close();
			results.remove(id);
			break;
		    }
		case 2: // prepare
		    {
			String id = readString(dataIn);
			String q = readString(dataIn);
			PreparedStatement s = connection.prepareStatement(q);
			stmts.put(id,s);
			break;
		    }
		case 3: // setLong
		    {
			String id = readString(dataIn);
			int a = dataIn.readInt();
			long b = dataIn.readLong();
			stmts.get(id).setLong(a,b);
			break;
		    }
		case 4: // setString
		    {
			String id = readString(dataIn);
			int a = dataIn.readInt();
			String b = readString(dataIn);
			stmts.get(id).setString(a,b);
			break;
		    }
		case 8: // setDouble
		    {
			String id = readString(dataIn);
			int a = dataIn.readInt();
			double b = dataIn.readDouble();
			stmts.get(id).setDouble(a,b);
			break;
		    }
		case 14: // set time
		    {
			String id = readString(dataIn);
			int a = dataIn.readInt();
			long b = dataIn.readLong();
			stmts.get(id).setTimestamp(a,new Timestamp(b));
			break;
		    }
		case 5: // execute
		    {
			String id = readString(dataIn);
			PreparedStatement s = stmts.get(id);
			try {
			    boolean r = s.execute();
			    if (r) {
				dataOut.writeByte(1);
				dataOut.flush(); // need to flush here, due to round-trip in this protocol :-(
				ResultSet rs = s.getResultSet();
				String id2 = readString(dataIn);
				results.put(id2,rs);
				ResultSetMetaData md = rs.getMetaData();
				int n = md.getColumnCount();
				dataOut.writeInt(n);
				for (int i=0; i<n; i++) {
				    writeString(dataOut,md.getColumnName(i+1));
				    writeString(dataOut,md.getColumnClassName(i+1));
				}
			    } else {
				dataOut.writeByte(0);
				int c = s.getUpdateCount();
				dataOut.writeInt(c);
			    }
			} catch (SQLException e) {
			    dataOut.writeByte(2);
			    writeString(dataOut,e.getMessage());
			}
			break;
		    }
		case 6: // next
		    {
			String id = readString(dataIn);
			ResultSet rs = results.get(id);
			if (rs.next()) {
			    dataOut.writeByte(1);
			} else {
			    dataOut.writeByte(0);
			}
			break;
		    }
		case 7: // get
		    {
			String id = readString(dataIn);
			int ind = dataIn.readInt();
			ResultSet rs = results.get(id);
			switch (dataIn.readByte()) {
			case 1: // int
			    {
				int i = rs.getInt(ind);
				if (rs.wasNull()) {
				    dataOut.writeByte(0);
				    continue;
				}
				dataOut.writeByte(1);
				dataOut.writeInt(i);
				break;
			    }
			case 2: // string
			    {
				String i = rs.getString(ind);
				if (rs.wasNull()) {
				    dataOut.writeByte(0);
				    continue;
				}
				dataOut.writeByte(1);
				writeString(dataOut,i);
				break;
			    }
			case 3: // double
			    {
				double i = rs.getDouble(ind);
				if (rs.wasNull()) {
				    dataOut.writeByte(0);
				    continue;
				}
				dataOut.writeByte(1);
				dataOut.writeDouble(i);
				break;
			    }
			case 4: // float
			    {
				float i = rs.getFloat(ind);
				if (rs.wasNull()) {
				    dataOut.writeByte(0);
				    continue;
				}
				dataOut.writeByte(1);
				dataOut.writeFloat(i);
				break;
			    }
			case 5: // time
			    {
				java.sql.Date i = rs.getDate(ind);
				if (rs.wasNull()) {
				    dataOut.writeByte(0);
				    continue;
				}
				dataOut.writeByte(1);
				dataOut.writeLong(i.getTime());
				break;
			    }
			case 11: // timestamp
			    {
				java.sql.Timestamp i = rs.getTimestamp(ind);
				if (rs.wasNull()) {
				    dataOut.writeByte(0);
				    continue;
				}
				dataOut.writeByte(1);
				dataOut.writeLong(i.getTime());
				break;
			    }
		        case 6: // long
			    {
				long i = rs.getLong(ind);
				if (rs.wasNull()) {
				    dataOut.writeByte(0);
				    continue;
				}
				dataOut.writeByte(1);
				dataOut.writeLong(i);
				break;
			    }
			case 7: // short
			    {
				short i = rs.getShort(ind);
				if (rs.wasNull()) {
				    dataOut.writeByte(0);
				    continue;
				}
				dataOut.writeByte(1);
				dataOut.writeShort(i);
				break;
			    }
			case 8: // byte
			    {
				byte i = rs.getByte(ind);
				if (rs.wasNull()) {
				    dataOut.writeByte(0);
				    continue;
				}
				dataOut.writeByte(1);
				dataOut.writeByte(i);
				break;
			    }
			case 9: // boolean
			    {
				boolean i = rs.getBoolean(ind);
				if (rs.wasNull()) {
				    dataOut.writeByte(0);
				    continue;
				}
				dataOut.writeByte(1);
				dataOut.writeByte(i ? 1 : 0);
				break;
			    }
			case 10: // big decimal
			    {
				java.math.BigDecimal i = rs.getBigDecimal(ind);
				if (rs.wasNull()) {
				    dataOut.writeByte(0);
				    continue;
				}
				dataOut.writeByte(1);
				writeString(dataOut,i.toString());
				break;
			    }
			}
			break;
		    }
		default:
		    throw new Exception("java unknown byte: " + selector);
		}
	    } finally {
		dataOut.flush();
	    }
	}
    }

}
`

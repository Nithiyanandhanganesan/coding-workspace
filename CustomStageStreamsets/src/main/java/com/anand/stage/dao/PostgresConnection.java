
public class PostgresConnection {
	public static void main(String args[])
	{
		HikariConfig config = new HikariConfig("/some/path/hikari.properties");
		HikariDataSource ds = new HikariDataSource(config);
	}
}

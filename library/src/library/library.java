package library;
import java.util.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Scanner;
import java.io.BufferedReader;
import java.io.File; 
import java.io.FileReader; 
import java.io.IOException;
import java.io.FileNotFoundException;

public class library{
	static Connection conn;
	public static void main(String args[]){
		
		start("root","czb201015");
	}
	public static void start(String userid, String passwd) { 
		try{ 
			Class.forName ("com.mysql.cj.jdbc.Driver");
			conn = DriverManager.getConnection(
					"jdbc:mysql://localhost:3306/mylibrary?characterEncoding=utf8&useSSL=true",
					userid, passwd);
	System.out.println("********************************************************");
			System.out.println("\t\t\t图书管理系统");
			System.out.println("********************************************************");
			
			Scanner reader=new Scanner(System.in);
			
			while(true){
			System.out.println("1.图书查询 2.管理员登陆 0.退出系统");
			System.out.println("请输入需要的服务编号:");
			int choice=reader.nextInt();
			switch(choice)
			{
			    case 0 :
					conn.close();
					return;
				case 1:
					check_Book(reader);
					break;
			    case 2 :
					int flag=log_in(reader);
					if (flag==1) {
						manager(reader);
					}
				    break;
				default:
					System.out.println("服务编号错误");
			}
			}
		}catch (Exception sqle){ 
			System.out.println("Exception : " + sqle);
		}
	}
	
	static int log_in(Scanner reader) throws SQLException
	{
		int ret=0;
		System.out.println("*********************************************************** *********************");
		System.out.println("\t\t\t登陆管理员账户");
		Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_UPDATABLE);
		String name, password;
		while(true) {
			System.out.println("请输入用户名(0. 退出）");
			name=reader.next();
			if (name.equals("0")) {
				break;
			}
			System.out.println("请输入密码");
			password=reader.next();
			ResultSet rset=stmt.executeQuery("select * from users where name=\""+name+"\"");
			if (rset.next()) {
				if (!rset.getString("password").equals(password)) {
					System.out.println("用户名或密码错误，请重新输入");
					continue;
				}
				else {
					ret=1;
					System.out.println("登陆成功！");
					System.out.println("*********************************************************** *********************");
					break;
				}
			}
		}
		return ret;
	}
	
	static void manager(Scanner reader) throws SQLException
	{
		while (true) {
			System.out.println("*********************************************************** *********************");
		System.out.println("\t\t\t管理员模式");
		System.out.println("1.借书管理 2.还书管理 3.图书入库 4.借阅证管理 0.登出管理员账户");
		System.out.println("请输入需要的服务编号:");
		int choice=reader.nextInt();
		switch(choice) {
		case 0 :
			return; 
		case 1 :
			borrow_Book(reader);
			break; 
		case 2:
			return_Book(reader);
			break; 
		case 3:
			add_Book(reader); 
			break; 
		case 4:
			proof_Manag(reader); 
			break; 
		default:
			System.out.println("服务编号错误");

		} 
		}
	}
	
	static void check_Book(Scanner reader) throws SQLException
	{
		String query;//查询语句
		int choice=0;//存放用户选项

		while(true){
		System.out.println("1.查询全部 2.按书名查询 3.按书号查询 4.按作者查询 5.按年份查询 6.按价格查询 0.退出");
		System.out.println("请输入需要的服务编号");
		choice=reader.nextInt();
		switch(choice)
		{
		    case 0 :
				return;
			case 1:
				//执行SQL语句
				query="SELECT * FROM book";
				book_query(query);
				break;  
			case 2:
				System.out.println("请输入要查询的书名"); 
				String title=reader.next(); 
				book_query("select * from book where book.title=\""+title+"\""); 
				break; 
			case 3:
				System.out.println("请输入要查询的书号"); 
				String bno=reader.next(); 
				book_query("select * from book where book.bno=\""+bno+"\"");
				break; 
			case 4:
				System.out.println("请输入要查询的作者");
				String author=reader.next();
				book_query("select * from book wherebook.author=\""+author+"\"");
				break; 
			case 5:
				System.out.println("请输入要查询的年份区间");
				int start=reader.nextInt(); 
				int end=reader.nextInt();
				book_query("select * from book where publish_year between "+start+" and "+end);
				break; 
			case 6:
				System.out.println("请输入要查询的价格区间"); 
				double lower=reader.nextDouble(); 
				double upper=reader.nextDouble();
				book_query("select * from book where price between "+lower+" and"+upper);
				break;
			default:
				System.out.println("服务编号错误");
				
		}
		}
	}
	static void book_query(String query) throws SQLException
	{
		Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_UPDATABLE);
		ResultSet rset = stmt.executeQuery(query);
		System.out.println("查询结果如下");
		System.out.println("*********************************************************** *********************");
		System.out.println("书号\t类别\t书名\t出版社\t\t年份\t作者\t价格\t总藏书量\t库 存");
		System.out.println("************************************************************ ********************");
		while (rset.next()) {
			System.out.println(rset.getString("bno")+"\t"+rset.getString("category")+ "\t"+rset.getString("title")+"\t"+rset.getString("press")+"\t"+rset.getInt("publish_year")+"\t"+rset.getString("author")+"\t"+rset.getDouble("price")+"\t"+rset.getInt("total")+"\t"+rset.getInt("stock") );
		}
		if(!rset.first()) { 
			System.out.println("未找到该书籍");
			return; 
		}
	}
	static void borrow_Book(Scanner reader) throws SQLException
	{
		Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_UPDATABLE);
		while(true) {
			System.out.println("请输入借阅证编号");
			String cno=reader.next();
			cardquery(cno);
			System.out.println("************************************************************ ********************");
			System.out.println("1.借书  0.退出");
			System.out.println("请输入需要的服务编号");
			int choice=reader.nextInt();
			switch(choice) {
			case 0:
				break;
			case 1:
				String bno=reader.next();
				ResultSet rset = stmt.executeQuery("select * from book where bno=\""+bno+"\"");
				if (rset.first()) {
					if (rset.getInt("stock")==0) {
						System.out.println("该书暂无库存");
						return;
					}
					else {
						int stock=rset.getInt("stock")-1; 
						stmt.executeUpdate("update book set stock= "+stock+" where bno=\""+bno+"\"");
					}
				}
				Calendar c1 = Calendar.getInstance();
				int month = c1.get(Calendar.MONTH) + 1;
				int date = c1.get(Calendar.DATE);
				int borrow_date=month*100+date;
				int return_date=0;
				stmt.executeUpdate("insert into borrow values(\"" +cno+"\",\""+bno+"\","+borrow_date+","+return_date+")");
				break;
			default:
				System.out.println("服务编号错误");
			}
		}
	}
	
	static void cardquery(String cno) throws SQLException
	{
		Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_UPDATABLE);
		String query="select * from borrow where cno=\""+cno+"\" having return_date = 0";
		ResultSet rset=stmt.executeQuery(query);
		System.out.println("借书记录如下");

		System.out.println("*********************************************************** *********************"); 
		System.out.println("书号\t类别\t书名\t出版社\t年份\t作者\t价格\t总藏书量\t库存");
		System.out.println("************************************************************ ********************");
		while(rset.next()) {
			query="select * from book where bno=\""+rset.getString("bno")+"\"";
			Statement stmt2=conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_UPDATABLE);
			ResultSet rset2=stmt2.executeQuery(query);
			if(rset2.first()) {
				System.out.println(rset2.getString("bno")+"\t"+rset2.getString("category")+
				"\t"+rset2.getString("title")+"\t"+rset2.getString("press")+
				"\t"+rset2.getInt("publish_year")+"\t"+rset2.getString("author")+
				"\t"+rset2.getDouble("price")+"\t"+rset2.getInt("total")+"\t"+rset2.getInt("stoc k"));
			}
		}
	}
	
	static void return_Book(Scanner reader) throws SQLException
	{
		Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_UPDATABLE);
		while(true) {
			System.out.println("请输入借阅证编号");
			String cno=reader.next();
			cardquery(cno);
			System.out.println("************************************************************ ********************");
			System.out.println("1.还书  0.返回");
			System.out.println("请输入需要的服务编号");
			int choice=reader.nextInt();
			switch(choice) {
			case 0:
				return;
			case 1:
				String bno=reader.next();
				ResultSet rset = stmt.executeQuery("select * from book where bno=\""+bno+"\"");
				if (rset.first()) {
					int stock=rset.getInt("stock")+1;
					stmt.executeUpdate("update book set stock= "+stock+" where bno=\""+bno+"\"");
				}
				Calendar c1 = Calendar.getInstance();
				int month = c1.get(Calendar.MONTH) + 1;
				int date = c1.get(Calendar.DATE);
				int return_date=month*100+date;
				stmt.executeUpdate("update borrow set return_date="+return_date+" where bno=\""+bno+"\"and cno=\""+cno+"\"");
			}
		}
	}
	static void add_Book(Scanner reader) throws SQLException
	{
		int choice=0;
		while(true){
			System.out.println("1.单本入库 2.批量入库 0.返回");
			System.out.println("请输入需要的服务编号");
			choice=reader.nextInt(); 
			switch(choice) {
			case 0:
				return;
			case 1:
				System.out.println("请输入书号,类别,书名,出版社,年份,作者,价格,数量");
				String bno,category,title,press,author; 
				double price;
				int year,number;
				bno=reader.next();
				category=reader.next();
				title=reader.next(); 
				press=reader.next();
				year=reader.nextInt();
				author=reader.next();
				price=reader.nextDouble();
				number=reader.nextInt();
				bookadd(bno,category,title,press,year,author,price,number); 
				break;
			case 2:
				try {
					System.out.println("请输入批量文件夹地址");
					String fileloc; fileloc=reader.next();
					System.out.println(fileloc); 
					fileadd(fileloc); 
				}
				catch(IOException io) {
					io.printStackTrace(); 
				} 
				break;
			default:
				System.out.println("服务编号错误");
			}
		}
	}
	
	static void fileadd(String fileloc) throws SQLException , IOException
	{
		File file=new File(fileloc);
		BufferedReader reader = null;
		try {
			reader= new BufferedReader(new FileReader(file));
		} 
		catch(FileNotFoundException e2){
			e2.printStackTrace(); 
		} 
		String newbook; 
		while((newbook=reader.readLine())!=null){
			Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_UPDATABLE);
			String add="insert into book values"+newbook;
			stmt.executeUpdate(add); 
		}
	}
	
	static void bookadd(String bno,String category,String title,String press,int year,String author,double price,int number) throws SQLException
	{
		Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_UPDATABLE);
		Statement stmt2 = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_UPDATABLE);
		String query="SELECT * FROM book where bno=\""+bno+"\"";
		ResultSet rset = stmt.executeQuery(query);
		rset.last();
		if(!rset.first()) { 
			stmt2.executeUpdate("insert into book values("+"\""+bno+"\",\""+category+"\",\""+title+"\",\""+press+"\","+year+",\""+ author+"\","+price+","+number+","+number+")");
		}
		else { 
			if(rset.getString("title")!=title) stmt2.executeUpdate("update book set title=\""+title+"\"where bno=\""+bno+"\"");

			if(rset.getString("press")!=press) stmt2.executeUpdate("update book set press=\""+press+"\"where bno=\""+bno+"\"");

			if(rset.getString("category")!=category) stmt2.executeUpdate("update book set category=\""+category+"\"where bno=\""+bno+"\"");

			if(rset.getString("author")!=author) stmt2.executeUpdate("update book set author=\""+author+"\"where bno=\""+bno+"\"");

			if(rset.getDouble("price")!=price) stmt2.executeUpdate("update book set price="+price+"where bno=\""+bno+"\"");

			if(rset.getInt("publish_year")!=year) stmt2.executeUpdate("update book set year="+year+"where bno=\""+bno+"\"");

			int total=rset.getInt("total")+number; 
			int stock=rset.getInt("stock")+number; 
			stmt2.executeUpdate("update book set total = "+total+" where bno=\""+bno+"\"");
			stmt2.executeUpdate("update book set stock = "+stock+" where bno=\""+bno+"\"");
		}
		rset.last(); 
		System.out.println("图书已入库");
	}
	
	static void card_insert(String cno,String reader_name,String department,String type) throws SQLException 
	{ 
		Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_UPDATABLE);
		stmt.executeUpdate("insert into card values("+"\""+cno+"\",\""+reader_name+"\",\""+department+"\",\""+type+"\" )");
		System.out.println("借阅证已添加"); 
	}
	
	static void card_delete(String cno) throws SQLException 
	{ 
		Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_UPDATABLE);
		stmt.executeUpdate("delete from card where cno =\""+cno+"\"");
		System.out.println("借阅证已删除"); 
	}
	
	static void proof_Manag(Scanner reader) throws SQLException
	{
		int choice=0;
		Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_UPDATABLE);
		while(true) {
			System.out.println("1.删除借阅证 2.增加借阅证 3.查看所有借阅证 0.返回");
			System.out.println("请输入需要的服务编号");
			choice=reader.nextInt(); 
			String cno,reader_name,department,type;
			switch(choice) 
			{
			case 0:
				return;
			case 1:
				System.out.println("请输入借书证卡号");
				cno=reader.next();
				card_delete(cno);
				break;
			case 2:
				System.out.println("请输入借书证卡号, 姓名, 单位, 类别 (教师T 学生S)");
				cno=reader.next(); 
				reader_name=reader.next();
				department=reader.next(); 
				type=reader.next();
				card_insert(cno,reader_name,department,type);
				break;
			case 3:
				ResultSet rset = stmt.executeQuery("select * from card");
				System.out.println("查询结果如下");
				System.out.println("*********************************************************** *********************"); 
				System.out.println("卡号\t姓名\t单位\t类别");
				System.out.println("************************************************************ ********************");
				while (rset.next()) {
					System.out.println(rset.getString("cno")+"\t"+rset.getString("reader_name")+"\t"+rset.getString("department")+"\t"+rset.getString("type"));
				} 
				break; 
			default:
				System.out.println("服务编号错误");
			}
		}
	}
	
}
	
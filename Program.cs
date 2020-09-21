using Npgsql;
using Dapper;
using System.Collections.Generic;
using System.Threading.Tasks;
using System;
using System.Linq;
using Newtonsoft.Json;
using System.Data;

namespace ReadJsonbArraysWithDtoInheritance
{
  class Program
  {
    static async Task Main(string[] args)
    {
      DefaultTypeMap.MatchNamesWithUnderscores = true;
      SqlMapper.AddTypeHandler(new NoteListTypeMapper());

      const string ConnectionString = "Host=localhost;Username=postgres;Port=5432;Password=postgres;Database=postgres";
      using(var connection = new NpgsqlConnection(ConnectionString))
      {
        await connection.OpenAsync(); // make sure all the code runs in the same session...

        await connection.ExecuteAsync("create temporary table product(id serial primary key, name text, notes jsonb);");
        await connection.ExecuteAsync("insert into product (name, notes) values ('ProductA', '[{\"id\": 1, \"content\": \"Note1 - A\"}, {\"id\": 2, \"content\": \"Note2 - A\"} ]'::jsonb);");
        await connection.ExecuteAsync("insert into product (name, notes) values ('ProductB', '[{\"id\": 2, \"content\": \"Note1 - B\"}, {\"id\": 2, \"content\": \"Note2 - B\"} ]'::jsonb);");

        // This loads the Notes jsonb column fine.
        List<Product> products = (await connection.QueryAsync<Product>("select * from product;")).ToList();
        Console.WriteLine(products[0]); // Prints: Product(1, ProductA): Note(1, Note1 - A), Note(2, Note2 - A)
        Console.WriteLine(products[1]); // Prints: Product(2, ProductB): Note(2, Note1 - B), Note(2, Note2 - B)

        // This does NOT load the Notes jsonb colum.
        List<ProductView> productViews = (await connection.QueryAsync<ProductView>("select * from product;")).ToList();
        Console.WriteLine(productViews[0]); // Prints: Product(1, ProductA):
        Console.WriteLine(productViews[1]); // Prints: Product(2, ProductB):
      }
    }

    internal class NoteListTypeMapper : SqlMapper.TypeHandler<IList<Note>>
    {
      public override IList<Note> Parse(object value)
      {
        if (value is string json && !string.IsNullOrEmpty(json))
        {
          return JsonConvert.DeserializeObject<IList<Note>>(json);
        }
        else
        {
          return new List<Note>();
        }
      }


      public override void SetValue(IDbDataParameter parameter, IList<Note> value)
      {
        parameter.Value = JsonConvert.SerializeObject(value);
        if (parameter is NpgsqlParameter postgresParameter)
        {
          postgresParameter.NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Jsonb;
        }
      }
    }
  }

  public class Note
  {
    public int Id { get; set; }
    public string Content { get; set; } = "";
    public override string ToString() => $"Note({this.Id}, {this.Content})";
  }


  public class Product
  {
    public int Id { get; set; }
    public string Name { get; set; } = "";
    public IList<Note> Notes { get; } = new List<Note>();

    public override string ToString() => $"Product({this.Id}, {this.Name}): " + string.Join(", ", this.Notes);
  }


  public class ProductView : Product
  {
    public string Manufacturer { get; set; } = "";
  }
}

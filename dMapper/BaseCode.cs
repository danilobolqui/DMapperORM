using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.Common;
using System.Reflection;
using System.ComponentModel;

namespace dMapper
{
    public class BaseCode<T> where T : EntityInterface.IEntity
    {
        public List<T> Ler(string camposSelect, string restricoesWhere, List<WhereParameter> parametrosRestricao, string camposOrdem)
        {
            return Ler(camposSelect, restricoesWhere, parametrosRestricao, camposOrdem, null, null);
        }

        public List<T> Ler(string camposSelect, string restricoesWhere, List<WhereParameter> parametrosRestricao, string camposOrdem, DbConnection conexao, DbTransaction trans)
        {
            //Cria StringBuilder.
            StringBuilder sb = new StringBuilder();

            //Escreve "*", caso não haja campos específicos.
            if (string.IsNullOrEmpty(camposSelect))
            {
                sb.Append("select *");
            }
            //Escreve "select", caso haja campos especificados.
            else
            {
                sb.Append("select ");
                sb.Append(camposSelect);
            }

            //Escreve from tabela.
            sb.Append(" from ");
            sb.Append(ObterNomeEntidadeBD());

            //Escreve "where", caso haja restrições where.
            if (!string.IsNullOrEmpty(restricoesWhere))
            {
                sb.Append(" where ");
                sb.Append(restricoesWhere);
            }

            //Escreve "order by", caso haja campos para ordenação.
            if (!string.IsNullOrEmpty(camposOrdem))
            {
                sb.Append(" order by ");
                sb.Append(camposOrdem);
            }

            //Cria conexão.
            DbConnection conexaoInterna;            

            //Caso a conexão seja instanciada internamente, liberar conexão após seu uso, caso a conexão seja instanciada externamente, o criador da conexão deve manipular a mesma.
            bool liberarConexao;

            //Verifica instancia da conexão recebida e valor para liberação da conexão.
            ReturnConnection retornoConn = VerificaInstanciaConexao(conexao);

            //Seta conexão interna.
            conexaoInterna = retornoConn.Connection;

            //Seta valor de liberação.
            liberarConexao = retornoConn.DisposeConnection;

            try
            {
                //Cria comando.
                using (DbCommand cmd = conexaoInterna.CreateCommand())
                {
                    //Dados do comando.
                    cmd.CommandText = sb.ToString();

                    if (!liberarConexao) { cmd.Transaction = trans; }

                    //Adiciona parameters.
                    if (parametrosRestricao != null)
                    {
                        foreach (WhereParameter parametro in parametrosRestricao)
                        {
                            DbParameter param = cmd.CreateParameter();
                            param.ParameterName = parametro.NomeParametro;
                            param.Value = parametro.Value;
                            cmd.Parameters.Add(param);                            
                        }
                    }

                    //Cria DataReader.
                    using (DbDataReader reader = cmd.ExecuteReader())
                    {
                        //Lista que armazenará os dados do BD para retorno do método.
                        List<T> listRetorno = new List<T>();

                        //Cria array com nome das colunas do DataReader.
                        string[] arrayNomeColunasDataReader = new string[reader.FieldCount];
                        for (int i = 0; i < reader.FieldCount; i++)
                        {
                            arrayNomeColunasDataReader[i] = reader.GetName(i);
                        }

                        //Obtém colunas em comum entre a Entity e o DataReader.
                        string[] colunasEmComumEntityDataReader = ObterColunasEmComumEntityDataReader<T>(arrayNomeColunasDataReader);

                        //Lê DataReader.
                        while (reader.Read())
                        {
                            //Cria instância da Entity.
                            object objData = Activator.CreateInstance(typeof(T));

                            //Popula propriedades da Entity.
                            foreach (string colunaComum in colunasEmComumEntityDataReader)
                            {
                                //Obtém propriedade da Entity pelo nome.
                                PropertyInfo propByName = typeof(T).GetProperty(colunaComum);

                                //Caso o valor do BD não seja nulo, setar valor.
                                if (reader[colunaComum] != DBNull.Value)
                                {
                                    //Tipo de conversão é setado inicialmente com type da propriedade.
                                    Type conversionType = propByName.PropertyType;

                                    //Caso o type da propriedade é nullable, alterar conversionType.
                                    if (propByName.PropertyType.IsGenericType && propByName.PropertyType.GetGenericTypeDefinition().Equals(typeof(Nullable<>)))
                                    {
                                        NullableConverter nullableConverter = new NullableConverter(conversionType);
                                        conversionType = nullableConverter.UnderlyingType;
                                    }

                                    //Seta valor na propriedade.
                                    propByName.SetValue(objData, Convert.ChangeType(reader[colunaComum], conversionType), null);
                                }
                            }

                            //Adiciona objeto na list de Entity.
                            listRetorno.Add((T)objData);
                        }

                        //Retorna lista.
                        return listRetorno;
                    }
                }
            }
            finally
            {
                //Libera recursos da conexão.
                if (liberarConexao)
                {
                    conexaoInterna.Close();
                    conexaoInterna.Dispose();
                }
            }
        }

        public int Inserir(T entidade)
        {
            return Inserir(entidade, null, null);
        }

        public int Inserir(T entidade, DbConnection conexao, DbTransaction trans)
        {
            //Obtém as propriedades da entity.
            PropertyInfo[] propriedadesEntity = typeof(T).GetProperties();

            //Escreve insert into tabela.
            StringBuilder sb = new StringBuilder("insert into ");
            sb.Append(ObterNomeEntidadeBD());

            //Escreve campos.
            sb.Append(" (");
            for (int i = 0; i < propriedadesEntity.Length; i++)
            {
                //Obtém PKs.
                object[] attributes = propriedadesEntity[i].GetCustomAttributes(typeof(dMapper.EntityAttributes.PrimaryKeyAttributes), false);

                //Cria insert com as seguintes condições:
                //É PK e Não Identity, ou não é PK.
                if (((attributes.Length > 0) && (!(attributes[0] as dMapper.EntityAttributes.PrimaryKeyAttributes).Identity)) || (attributes.Length == 0))
                {
                    sb.Append(propriedadesEntity[i].Name);

                    if (i != propriedadesEntity.Length - 1)
                    {
                        sb.Append(",");
                    }
                }

            }
            sb.Append(")");

            //Escreve values.
            sb.Append(" values ");

            //Escreve parâmetros.
            sb.Append(" (");
            for (int i = 0; i < propriedadesEntity.Length; i++)
            {
                //Obtém PKs.
                object[] attributes = propriedadesEntity[i].GetCustomAttributes(typeof(dMapper.EntityAttributes.PrimaryKeyAttributes), false);

                //Cria insert com as seguintes condições:
                //É PK e Não Identity, ou não é PK.
                if (((attributes.Length > 0) && (!(attributes[0] as dMapper.EntityAttributes.PrimaryKeyAttributes).Identity)) || (attributes.Length == 0))
                {
                    sb.Append("@");
                    sb.Append(propriedadesEntity[i].Name); ;

                    if (i != propriedadesEntity.Length - 1)
                    {
                        sb.Append(",");
                    }
                }
            }
            sb.Append(")");

            //****************************************************
            //Para bancos como Oracle e Firebird, será necessário identificar o campo auto increment para popular seu valor no insert, após rodar a sequence/generator.
            //Já está implementado na entity a identificação de campos auto increment.
            //Para fazer isso, criar um método parecido com o ObterPrimaryKey(), mas que retorne apenas o campo identity, e ao inserir o valor dos parametros, comparar o nome do campo identity com o nome da propriedade, se forem iguais, usar o valor da sequence.
            //****************************************************

            //Obtém ProviderName da ConnectionString.
            ProviderName provider = ConnectionProvider.GetProviderName(ObterNomeStringConexaoEntidade());

            if (provider == ProviderName.SystemDataSqlClient)
            {
                sb.Append("; select Scope_Identity();");
            }
            else if (provider == ProviderName.MySqlDataMySqlClient)
            {
                sb.Append("; select Last_Insert_Id();");
            }
            else
            {
                throw new ArgumentException("Provider não suportado.");
            }

            //Cria conexão.
            DbConnection conexaoInterna;

            //Caso a conexão seja instanciada internamente, liberar conexão após seu uso, caso a conexão seja instanciada externamente, o criador da conexão deve manipular a mesma.
            bool liberarConexao;

            //Verifica instancia da conexão recebida e valor para liberação da conexão.
            ReturnConnection retornoConn = VerificaInstanciaConexao(conexao);

            //Seta conexão interna.
            conexaoInterna = retornoConn.Connection;

            //Seta valor de liberação.
            liberarConexao = retornoConn.DisposeConnection;

            //Transação. (Só é usada quando é necessário liberar a conexão neste escopo).
            DbTransaction transInt = null;

            //Inicia transação.
            if (liberarConexao)
            {
                transInt = conexaoInterna.BeginTransaction();
            }
            else
            {
                transInt = trans;
            }

            try
            {
                //Cria comando.
                using (DbCommand cmd = conexaoInterna.CreateCommand())
                {
                    //Dados do comando.
                    cmd.CommandText = sb.ToString();
                    cmd.Transaction = transInt;

                    //Escreve os valores dos parâmetros.
                    for (int i = 0; i < propriedadesEntity.Length; i++)
                    {
                        //Obtém PKs.
                        object[] attributes = propriedadesEntity[i].GetCustomAttributes(typeof(dMapper.EntityAttributes.PrimaryKeyAttributes), false);

                        //Cria insert com as seguintes condições:
                        //É PK e Não Identity, ou não é PK.
                        if (((attributes.Length > 0) && (!(attributes[0] as dMapper.EntityAttributes.PrimaryKeyAttributes).Identity)) || (attributes.Length == 0))
                        {
                            string nomeParametro = string.Concat("@", propriedadesEntity[i].Name);
                            object valorParametro = propriedadesEntity[i].GetValue(entidade, null);

                            //Seta DbNull.Value.
                            if (valorParametro == null)
                            {
                                valorParametro = DBNull.Value;
                            }
                            else if (valorParametro is string)
                            {
                                if (string.IsNullOrEmpty(valorParametro.ToString()))
                                {
                                    valorParametro = DBNull.Value;
                                }
                            }

                            DbParameter param = cmd.CreateParameter();
                            param.ParameterName = nomeParametro;
                            param.Value = valorParametro;
                            cmd.Parameters.Add(param);
                        }
                    }

                    //Executa insert e retorna id.
                    object idObj = cmd.ExecuteScalar();

                    //Id para retorno.
                    int id = 0;

                    //Preenche id caso retorne algum valor.
                    if ((idObj != DBNull.Value) && (idObj != null))
                    {
                        id = Convert.ToInt32(idObj);
                    }

                    //Comita transação.
                    if (liberarConexao) { transInt.Commit(); }

                    //Retorna id.
                    return id;
                }
            }
            catch (Exception ex)
            {
                //Rolllback na transação.
                if (liberarConexao) { transInt.Rollback(); }

                //Lança exceção.
                throw new Exception(string.Concat("Erro ao executar insert. Erro:\n", ex.Message));
            }
            finally
            {
                //Libera recursos da conexão.
                if (liberarConexao)
                {
                    conexaoInterna.Close();
                    conexaoInterna.Dispose();
                }
            }
        }

        public void Atualizar(T entidade)
        {
            Atualizar(entidade, null, null, null, null);
        }

        public void Atualizar(T entidade, DbConnection conexao, DbTransaction trans)
        {
            Atualizar(entidade, null, null, conexao, trans);
        }

        public void Atualizar(T entidade, string wherePKComposta, List<WhereParameter> parametrosWherePKComposta)
        {
            Atualizar(entidade, wherePKComposta, parametrosWherePKComposta, null, null);
        }

        public void Atualizar(T entidade, string wherePKComposta, List<WhereParameter> parametrosWherePKComposta, DbConnection conexao, DbTransaction trans)
        {
            //Obtém as propriedades da entity.
            PropertyInfo[] propriedadesEntity = typeof(T).GetProperties();

            //Escreve update tabela set.
            StringBuilder sb = new StringBuilder("update ");
            sb.Append(ObterNomeEntidadeBD());
            sb.Append(" set");

            //Escreve campo=@campo.
            for (int i = 0; i < propriedadesEntity.Length; i++)
            {
                //Obtém PKs.
                object[] attributes = propriedadesEntity[i].GetCustomAttributes(typeof(dMapper.EntityAttributes.PrimaryKeyAttributes), false);

                //Cria update com as seguintes condições:
                //É PK e Não Identity, ou não é PK.
                if (((attributes.Length > 0) && (!(attributes[0] as dMapper.EntityAttributes.PrimaryKeyAttributes).Identity)) || (attributes.Length == 0))
                {
                    string nomePropriedade = propriedadesEntity[i].Name;

                    sb.Append(" ");
                    sb.Append(nomePropriedade);
                    sb.Append("=@");
                    sb.Append(nomePropriedade);

                    if (i != propriedadesEntity.Length - 1)
                    {
                        sb.Append(",");
                    }
                }
            }

            //Escreve where.
            sb.Append(" where ");

            //Obtém PKs(Registrada na entity como atributo).
            string[] primaryKeys = ObterPrimaryKey();

            //Caso a chave seja composta, usar where passado como parâmetro, específico para PK composta.
            //No caso de PK composta a entidade passada como referência, contém os valores de alteração, mas não contém os valores do registro atual, que são usados para geração do where.
            if (string.IsNullOrEmpty(wherePKComposta))
            {
                //Escreve campo=@campo(where com as PKs da entidade).
                for (int i = 0; i < primaryKeys.Length; i++)
                {
                    if (i > 0)
                    {
                        sb.Append(" and ");
                    }

                    sb.Append(primaryKeys[i]);
                    sb.Append("=@");
                    sb.Append(primaryKeys[i]);
                }
            }
            else
            {
                sb.Append(wherePKComposta);
            }

            //Cria conexão.
            DbConnection conexaoInterna;

            //Caso a conexão seja instanciada internamente, liberar conexão após seu uso, caso a conexão seja instanciada externamente, o criador da conexão deve manipular a mesma.
            bool liberarConexao;

            //Verifica instancia da conexão recebida e valor para liberação da conexão.
            ReturnConnection retornoConn = VerificaInstanciaConexao(conexao);

            //Seta conexão interna.
            conexaoInterna = retornoConn.Connection;

            //Seta valor de liberação.
            liberarConexao = retornoConn.DisposeConnection;

            //Transação. (Só é usada quando é necessário liberar a conexão neste escopo).
            DbTransaction transInt = null;

            //Inicia transação.
            if (liberarConexao)
            {
                transInt = conexaoInterna.BeginTransaction();
            }
            else
            {
                transInt = trans;
            }

            try
            {
                //Cria comando.
                using (DbCommand cmd = conexaoInterna.CreateCommand())
                {
                    //Dados do comando.
                    cmd.CommandText = sb.ToString();
                    cmd.Transaction = transInt;

                    //Escreve os valores dos parâmetros.
                    //Obs. Os parâmetros das PKs são os mesmos usados nos campos para update.
                    for (int i = 0; i < propriedadesEntity.Length; i++)
                    {
                        string nomeParametro = string.Concat("@", propriedadesEntity[i].Name);
                        object valorParametro = propriedadesEntity[i].GetValue(entidade, null);

                        //Seta DbNull.Value.
                        if (valorParametro == null)
                        {
                            valorParametro = DBNull.Value;
                        }
                        else if (valorParametro is string)
                        {
                            if (string.IsNullOrEmpty(valorParametro.ToString()))
                            {
                                valorParametro = DBNull.Value;
                            }
                        }
                        
                        DbParameter param = cmd.CreateParameter();
                        param.ParameterName = nomeParametro;
                        param.Value = valorParametro;
                        cmd.Parameters.Add(param);
                    }

                    //Escreve parâmetros do where caso a chave seja composta.
                    if (parametrosWherePKComposta != null)
                    {
                        foreach (WhereParameter paramPKComp in parametrosWherePKComposta)
                        {
                            string nomeParametro = paramPKComp.NomeParametro;
                            object valorParametro = paramPKComp.Value;
                            if (valorParametro == null) valorParametro = DBNull.Value;
                            DbParameter param = cmd.CreateParameter();
                            param.ParameterName = nomeParametro;
                            param.Value = valorParametro;
                            cmd.Parameters.Add(param);
                        }
                    }

                    //Executa insert.
                    cmd.ExecuteNonQuery();

                    //Comita transação.
                    if (liberarConexao) { transInt.Commit(); }
                }
            }
            catch (Exception ex)
            {
                //Rolllback na transação.
                if (liberarConexao) { transInt.Rollback(); }

                //Lança exceção.
                throw new Exception(string.Concat("Erro ao executar update. Erro:\n", ex.Message));
            }
            finally
            {
                //Libera recursos da conexão.
                if (liberarConexao)
                {
                    conexaoInterna.Close();
                    conexaoInterna.Dispose();
                }
            }
        }

        public void Excluir(T entidade)
        {
            Excluir(entidade, null, null);
        }

        public void Excluir(T entidade, DbConnection conexao, DbTransaction trans)
        {
            //Escreve delete from.
            StringBuilder sb = new StringBuilder("delete from ");

            //Escreve nome da tabela.
            sb.Append(ObterNomeEntidadeBD());

            //Escreve where.
            sb.Append(" where ");

            //Obtém PKs(Registrada na entity como atributo).
            string[] primaryKeys = ObterPrimaryKey();

            //Escreve campo=@campo(where com as PKs da entidade).
            for (int i = 0; i < primaryKeys.Length; i++)
            {
                if (i > 0)
                {
                    sb.Append(" and ");
                }

                sb.Append(primaryKeys[i]);
                sb.Append("=@");
                sb.Append(primaryKeys[i]);
            }

            //Cria conexão.
            DbConnection conexaoInterna;

            //Caso a conexão seja instanciada internamente, liberar conexão após seu uso, caso a conexão seja instanciada externamente, o criador da conexão deve manipular a mesma.
            bool liberarConexao;

            //Verifica instancia da conexão recebida e valor para liberação da conexão.
            ReturnConnection retornoConn = VerificaInstanciaConexao(conexao);

            //Seta conexão interna.
            conexaoInterna = retornoConn.Connection;

            //Seta valor de liberação.
            liberarConexao = retornoConn.DisposeConnection;

            //Transação. (Só é usada quando é necessário liberar a conexão neste escopo).
            DbTransaction transInt = null;

            //Inicia transação.
            if (liberarConexao)
            {
                transInt = conexaoInterna.BeginTransaction();
            }
            else
            {
                transInt = trans;
            }

            try
            {
                //Cria comando.
                using (DbCommand cmd = conexaoInterna.CreateCommand())
                {
                    //Dados do comando.
                    cmd.CommandText = sb.ToString();
                    cmd.Transaction = transInt;

                    //Escreve valores dos parâmetros where. 
                    foreach (string pk in primaryKeys)
                    {
                        string nomeParametro = string.Concat("@", pk);
                        object valorParametro = typeof(T).GetProperty(pk).GetValue(entidade, null);
                        if (valorParametro == null) valorParametro = DBNull.Value;
                        DbParameter param = cmd.CreateParameter();
                        param.ParameterName = nomeParametro;
                        param.Value = valorParametro;
                        cmd.Parameters.Add(param);
                    }

                    //Executa insert.
                    cmd.ExecuteNonQuery();

                    //Comita transação.
                    if (liberarConexao) { transInt.Commit(); }
                }
            }
            catch (Exception ex)
            {
                //Rolllback na transação.
                if (liberarConexao) { transInt.Rollback(); }

                //Lança exceção.
                throw new Exception(string.Concat("Erro ao executar delete. Erro:\n", ex.Message));
            }
            finally
            {
                //Libera recursos da conexão.
                if (liberarConexao)
                {
                    conexaoInterna.Close();
                    conexaoInterna.Dispose();
                }
            }
        }

        public void Excluir(string restricoesWhere, List<WhereParameter> parametrosRestricao)
        {
            Excluir(restricoesWhere, parametrosRestricao, null, null);
        }

        public void Excluir(string restricoesWhere, List<WhereParameter> parametrosRestricao, DbConnection conexao, DbTransaction trans)
        {
            //Escreve delete from.
            StringBuilder sb = new StringBuilder("delete from ");

            //Escreve nome da tabela.
            sb.Append(ObterNomeEntidadeBD());

            //Escreve "where", caso haja restrições where.
            if (!string.IsNullOrEmpty(restricoesWhere)) restricoesWhere = string.Concat(" where ", restricoesWhere);

            //Cria conexão.
            DbConnection conexaoInterna;

            //Caso a conexão seja instanciada internamente, liberar conexão após seu uso, caso a conexão seja instanciada externamente, o criador da conexão deve manipular a mesma.
            bool liberarConexao;

            //Verifica instancia da conexão recebida e valor para liberação da conexão.
            ReturnConnection retornoConn = VerificaInstanciaConexao(conexao);

            //Seta conexão interna.
            conexaoInterna = retornoConn.Connection;

            //Seta valor de liberação.
            liberarConexao = retornoConn.DisposeConnection;

            //Transação. (Só é usada quando é necessário liberar a conexão neste escopo).
            DbTransaction transInt = null;

            //Inicia transação.
            if (liberarConexao)
            {
                transInt = conexaoInterna.BeginTransaction();
            }
            else
            {
                transInt = trans;
            }

            try
            {
                //Cria comando.
                using (DbCommand cmd = conexaoInterna.CreateCommand())
                {
                    //Dados do comando.
                    cmd.CommandText = string.Concat(sb.ToString(), restricoesWhere);
                    cmd.Transaction = transInt;

                    //Adiciona parameters.
                    if (parametrosRestricao != null)
                    {
                        foreach (WhereParameter parametro in parametrosRestricao)
                        {
                            DbParameter param = cmd.CreateParameter();
                            param.ParameterName = parametro.NomeParametro;
                            param.Value = parametro.Value;
                            cmd.Parameters.Add(param);
                        }
                    }

                    //Executa insert.
                    cmd.ExecuteNonQuery();

                    //Comita transação.
                    if (liberarConexao) { transInt.Commit(); }
                }
            }
            catch (Exception ex)
            {
                //Rolllback na transação.
                if (liberarConexao) { transInt.Rollback(); }

                //Lança exceção.
                throw new Exception(string.Concat("Erro ao executar delete. Erro:\n", ex.Message));
            }
            finally
            {
                //Libera recursos da conexão.
                if (liberarConexao)
                {
                    conexaoInterna.Close();
                    conexaoInterna.Dispose();
                }
            }
        }

        public void ExecutarProcedure(string nomeProcedure, List<WhereParameter> parametros)
        {
            ExecutarProcedure(nomeProcedure, parametros, null, null);
        }

        public void ExecutarProcedure(string nomeProcedure, List<WhereParameter> parametros, DbConnection conexao, DbTransaction trans)
        {
            //Cria conexão.
            DbConnection conexaoInterna;

            //Caso a conexão seja instanciada internamente, liberar conexão após seu uso, caso a conexão seja instanciada externamente, o criador da conexão deve manipular a mesma.
            bool liberarConexao;

            //Verifica instancia da conexão recebida e valor para liberação da conexão.
            ReturnConnection retornoConn = VerificaInstanciaConexao(conexao);

            //Seta conexão interna.
            conexaoInterna = retornoConn.Connection;

            //Seta valor de liberação.
            liberarConexao = retornoConn.DisposeConnection;

            try
            {
                //Cria comando.
                using (DbCommand cmd = conexaoInterna.CreateCommand())
                {
                    //Dados do comando.
                    cmd.CommandText = nomeProcedure;
                    cmd.CommandType = System.Data.CommandType.StoredProcedure;

                    //Adiciona parameters.
                    if (parametros != null)
                    {
                        foreach (WhereParameter parametro in parametros)
                        {
                            DbParameter param = cmd.CreateParameter();
                            param.ParameterName = parametro.NomeParametro;
                            object valorParametro = parametro.Value;

                            //Seta DbNull.Value.
                            if (valorParametro == null)
                            {
                                valorParametro = DBNull.Value;
                            }
                            else if (valorParametro is string)
                            {
                                if (string.IsNullOrEmpty(valorParametro.ToString()))
                                {
                                    valorParametro = DBNull.Value;
                                }
                            }
                            
                            param.Value = valorParametro;
                            cmd.Parameters.Add(param);
                        }
                    }

                    cmd.ExecuteNonQuery();
                }
            }
            finally
            {
                //Libera recursos da conexão.
                if (liberarConexao)
                {
                    conexaoInterna.Close();
                    conexaoInterna.Dispose();
                }
            }            
        }

        public List<T2> ExecutarProcedure<T2>(string nomeProcedure, List<WhereParameter> parametros)
        {
            return ExecutarProcedure<T2>(nomeProcedure, parametros, null, null);
        }

        public List<T2> ExecutarProcedure<T2>(string nomeProcedure, List<WhereParameter> parametros, DbConnection conexao, DbTransaction trans)
        {
            //Cria conexão.
            DbConnection conexaoInterna;

            //Caso a conexão seja instanciada internamente, liberar conexão após seu uso, caso a conexão seja instanciada externamente, o criador da conexão deve manipular a mesma.
            bool liberarConexao;

            //Verifica instancia da conexão recebida e valor para liberação da conexão.
            ReturnConnection retornoConn = VerificaInstanciaConexao(conexao);

            //Seta conexão interna.
            conexaoInterna = retornoConn.Connection;

            //Seta valor de liberação.
            liberarConexao = retornoConn.DisposeConnection;

            try
            {
                //Cria comando.
                using (DbCommand cmd = conexaoInterna.CreateCommand())
                {
                    //Dados do comando.
                    cmd.CommandText = nomeProcedure;
                    cmd.CommandType = System.Data.CommandType.StoredProcedure;

                    //Caso conexão seja externa, usar transação recebida.
                    if (!liberarConexao)
                    {
                        cmd.Transaction = trans;
                    }

                    //Adiciona parameters.
                    if (parametros != null)
                    {
                        foreach (WhereParameter parametro in parametros)
                        {
                            DbParameter param = cmd.CreateParameter();
                            param.ParameterName = parametro.NomeParametro;
                            object valorParametro = parametro.Value;

                            //Seta DbNull.Value.
                            if (valorParametro == null)
                            {
                                valorParametro = DBNull.Value;
                            }
                            else if (valorParametro is string)
                            {
                                if (string.IsNullOrEmpty(valorParametro.ToString()))
                                {
                                    valorParametro = DBNull.Value;
                                }
                            }                            
                            
                            param.Value = valorParametro;
                            cmd.Parameters.Add(param);
                        }
                    }

                    //Cria DataReader.
                    using (DbDataReader reader = cmd.ExecuteReader())
                    {
                        //Lista que armazenará os dados do BD para retorno do método.
                        List<T2> listRetorno = new List<T2>();

                        //Cria array com nome das colunas do DataReader.
                        string[] arrayNomeColunasDataReader = new string[reader.FieldCount];
                        for (int i = 0; i < reader.FieldCount; i++)
                        {
                            arrayNomeColunasDataReader[i] = reader.GetName(i);
                        }

                        //Obtém colunas em comum entre a Entity e o DataReader.
                        string[] colunasEmComumEntityDataReader = ObterColunasEmComumEntityDataReader<T2>(arrayNomeColunasDataReader);

                        //Lê DataReader.
                        while (reader.Read())
                        {
                            //Cria instância da Entity.
                            object objData = Activator.CreateInstance(typeof(T2));

                            //Popula propriedades da Entity.
                            foreach (string colunaComum in colunasEmComumEntityDataReader)
                            {
                                //Obtém propriedade da Entity pelo nome.
                                PropertyInfo propByName = typeof(T2).GetProperty(colunaComum);

                                //Caso o valor do BD não seja nulo, setar valor.
                                if (reader[colunaComum] != DBNull.Value)
                                {
                                    //Tipo de conversão é setado inicialmente com type da propriedade.
                                    Type conversionType = propByName.PropertyType;

                                    //Caso o type da propriedade é nullable, alterar conversionType.
                                    if (propByName.PropertyType.IsGenericType && propByName.PropertyType.GetGenericTypeDefinition().Equals(typeof(Nullable<>)))
                                    {
                                        NullableConverter nullableConverter = new NullableConverter(conversionType);
                                        conversionType = nullableConverter.UnderlyingType;
                                    }

                                    //Seta valor na propriedade.
                                    propByName.SetValue(objData, Convert.ChangeType(reader[colunaComum], conversionType), null);
                                }
                            }

                            //Adiciona objeto na list de Entity.
                            listRetorno.Add((T2)objData);
                        }

                        //Retorna lista.
                        return listRetorno;
                    }
                }
            }
            finally
            {
                //Libera recursos da conexão.
                if (liberarConexao)
                {
                    conexaoInterna.Close();
                    conexaoInterna.Dispose();
                }
            }
        }

        public List<T2> ExecutarSelect<T2>(string sql, List<WhereParameter> parametrosRestricao)
        {
            return ExecutarSelect<T2>(sql, parametrosRestricao, null, null);
        }

        public List<T2> ExecutarSelect<T2>(string sql, List<WhereParameter> parametrosRestricao, DbConnection conexao, DbTransaction trans)
        {
            //Cria conexão.
            DbConnection conexaoInterna;

            //Caso a conexão seja instanciada internamente, liberar conexão após seu uso, caso a conexão seja instanciada externamente, o criador da conexão deve manipular a mesma.
            bool liberarConexao;

            //Verifica instancia da conexão recebida e valor para liberação da conexão.
            ReturnConnection retornoConn = VerificaInstanciaConexao(conexao);

            //Seta conexão interna.
            conexaoInterna = retornoConn.Connection;

            //Seta valor de liberação.
            liberarConexao = retornoConn.DisposeConnection;

            try
            {
                //Cria comando.
                using (DbCommand cmd = conexaoInterna.CreateCommand())
                {
                    //Dados do comando.
                    cmd.CommandText = sql;

                    if (!liberarConexao) { cmd.Transaction = trans; }

                    //Adiciona parameters.
                    if (parametrosRestricao != null)
                    {
                        foreach (WhereParameter parametro in parametrosRestricao)
                        {
                            DbParameter param = cmd.CreateParameter();
                            param.ParameterName = parametro.NomeParametro;
                            param.Value = parametro.Value;
                            cmd.Parameters.Add(param);
                        }
                    }

                    //Cria DataReader.
                    using (DbDataReader reader = cmd.ExecuteReader())
                    {
                        //Lista que armazenará os dados do BD para retorno do método.
                        List<T2> listRetorno = new List<T2>();

                        //Cria array com nome das colunas do DataReader.
                        string[] arrayNomeColunasDataReader = new string[reader.FieldCount];
                        for (int i = 0; i < reader.FieldCount; i++)
                        {
                            arrayNomeColunasDataReader[i] = reader.GetName(i);
                        }

                        //Obtém colunas em comum entre a Entity e o DataReader.
                        string[] colunasEmComumEntityDataReader = ObterColunasEmComumEntityDataReader<T2>(arrayNomeColunasDataReader);

                        //Lê DataReader.
                        while (reader.Read())
                        {
                            //Cria instância da Entity.
                            object objData = Activator.CreateInstance(typeof(T2));

                            //Popula propriedades da Entity.
                            foreach (string colunaComum in colunasEmComumEntityDataReader)
                            {
                                //Obtém propriedade da Entity pelo nome.
                                PropertyInfo propByName = typeof(T2).GetProperty(colunaComum);

                                //Caso o valor do BD não seja nulo, setar valor.
                                if (reader[colunaComum] != DBNull.Value)
                                {
                                    //Tipo de conversão é setado inicialmente com type da propriedade.
                                    Type conversionType = propByName.PropertyType;

                                    //Caso o type da propriedade é nullable, alterar conversionType.
                                    if (propByName.PropertyType.IsGenericType && propByName.PropertyType.GetGenericTypeDefinition().Equals(typeof(Nullable<>)))
                                    {
                                        NullableConverter nullableConverter = new NullableConverter(conversionType);
                                        conversionType = nullableConverter.UnderlyingType;
                                    }

                                    //Seta valor na propriedade.
                                    propByName.SetValue(objData, Convert.ChangeType(reader[colunaComum], conversionType), null);
                                }
                            }

                            //Adiciona objeto na list de Entity.
                            listRetorno.Add((T2)objData);
                        }

                        //Retorna lista.
                        return listRetorno;
                    }
                }
            }
            finally
            {
                //Libera recursos da conexão.
                if (liberarConexao)
                {
                    conexaoInterna.Close();
                    conexaoInterna.Dispose();
                }
            }
        }

        public void ExecutarComandoSemRetorno(string sql, List<WhereParameter> parametrosRestricao)
        {
            ExecutarComandoSemRetorno(sql, parametrosRestricao, null, null);
        }

        public void ExecutarComandoSemRetorno(string sql, List<WhereParameter> parametrosRestricao, DbConnection conexao, DbTransaction trans)
        {
            //Cria conexão.
            DbConnection conexaoInterna;

            //Caso a conexão seja instanciada internamente, liberar conexão após seu uso, caso a conexão seja instanciada externamente, o criador da conexão deve manipular a mesma.
            bool liberarConexao;

            //Verifica instancia da conexão recebida e valor para liberação da conexão.
            ReturnConnection retornoConn = VerificaInstanciaConexao(conexao);

            //Seta conexão interna.
            conexaoInterna = retornoConn.Connection;

            //Seta valor de liberação.
            liberarConexao = retornoConn.DisposeConnection;

            //Transação. (Só é usada quando é necessário liberar a conexão neste escopo).
            DbTransaction transInt = null;

            //Inicia transação.
            if (liberarConexao)
            {
                transInt = conexaoInterna.BeginTransaction();
            }
            else
            {
                transInt = trans;
            }

            try
            {
                //Cria comando.
                using (DbCommand cmd = conexaoInterna.CreateCommand())
                {
                    //Dados do comando.
                    cmd.CommandText = sql;
                    cmd.Transaction = transInt;

                    //Adiciona parameters.
                    if (parametrosRestricao != null)
                    {
                        foreach (WhereParameter parametro in parametrosRestricao)
                        {
                            DbParameter param = cmd.CreateParameter();
                            param.ParameterName = parametro.NomeParametro;
                            param.Value = parametro.Value;
                            cmd.Parameters.Add(param);
                        }
                    }

                    //Executa insert.
                    cmd.ExecuteNonQuery();

                    //Comita transação.
                    if (liberarConexao) { transInt.Commit(); }
                }
            }
            catch (Exception ex)
            {
                //Rolllback na transação.
                if (liberarConexao) { transInt.Rollback(); }

                //Lança exceção.
                throw new Exception(string.Concat("Erro ao executar comando. Erro:\n", ex.Message));
            }
            finally
            {
                //Libera recursos da conexão.
                if (liberarConexao)
                {
                    conexaoInterna.Close();
                    conexaoInterna.Dispose();
                }
            }
        }

        private string ObterNomeEntidadeBD()
        {
            return ((dMapper.EntityInterface.IEntity)Activator.CreateInstance(typeof(T))).Mapeador_NomeEntidade();
        }

        private string ObterNomeStringConexaoEntidade()
        {
            return ((dMapper.EntityInterface.IEntity)Activator.CreateInstance(typeof(T))).Mapeador_NomeStringConexao();
        }

        private string[] ObterColunasEmComumEntityDataReader<T2>(string[] colunasDataReader)
        {
            //Cria array vazio que será populado e usado para retorno do método.
            string[] colunasEmComum = new string[0];

            //Obtém propriedades da entity.
            PropertyInfo[] propriedadesEntity = typeof(T2).GetProperties();

            //A prioridade são as propriedades da Entity.
            foreach (PropertyInfo propriedadeEntity in propriedadesEntity)
            {
                //Para cada propriedade da Entity, verificar se existe valor correspondente no DataReader.
                foreach (string colunaDataReader in colunasDataReader)
                {
                    //Caso existir correspondência.
                    if (propriedadeEntity.Name.ToUpper() == colunaDataReader.ToUpper())
                    {
                        //Redimensiona array, com mais uma posição.
                        System.Array.Resize(ref colunasEmComum, colunasEmComum.Length + 1);

                        //Popula ultima posição do array.
                        colunasEmComum[colunasEmComum.Length - 1] = propriedadeEntity.Name;
                    }
                }
            }

            //Retorna lista em comum.
            return colunasEmComum;
        }

        private string[] ObterPrimaryKey()
        {
            //Array usado para retorno do método.
            string[] primaryKeys = new string[0];

            //Obtém propriedades da entity.
            PropertyInfo[] propInfos = typeof(T).GetProperties();

            //Faz loop nas propriedades da entity procurando por atributos.
            foreach (PropertyInfo propInfo in propInfos)
            {
                //Obtém atributos da propriedade.
                object[] attributes = propInfo.GetCustomAttributes(typeof(dMapper.EntityAttributes.PrimaryKeyAttributes), false);

                //Faz loop nos atributos para popular o retorno da função.
                foreach (object attribute in attributes)
                {
                    //Redimensiona array, com mais uma posição.
                    System.Array.Resize(ref primaryKeys, primaryKeys.Length + 1);

                    //Popula ultima posição do array.
                    primaryKeys[primaryKeys.Length - 1] = (attribute as dMapper.EntityAttributes.PrimaryKeyAttributes).NomePrimaryKey;
                }
            }

            //Retorna PrimaryKeys.
            return primaryKeys;
        }

        private ReturnConnection VerificaInstanciaConexao(DbConnection conexao)
        {
            //Objeto de retorno.
            ReturnConnection retornoConn = new ReturnConnection();

            //Instancia conexão.
            if (conexao == null)
            {
                //Obtém conexão pelo nome da conexão. Caso o valor recebido seja null ou vazio, o nome da conexão será o nome de conexão padrão.
                retornoConn.Connection = ConnectionProvider.GetConnection(ObterNomeStringConexaoEntidade());

                //Seta valor de liberação da conexão.
                retornoConn.DisposeConnection = true;
            }
            else
            {
                //Usa conexão recebida.
                retornoConn.Connection = conexao;

                //Seta valor de liberação da conexão.
                retornoConn.DisposeConnection = false;
            }

            //Retorno.
            return retornoConn;
        }
    }
}
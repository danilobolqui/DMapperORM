using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace dMapper
{
    public class WhereParameter
    {
        #region Construtores
        public WhereParameter()
        {
        }

        public WhereParameter(string nomeParametro, object value)
        {
            NomeParametro = nomeParametro;
            Value = value;
        }
        #endregion

        #region Propriedades
        public string NomeParametro { get; set; }
        public object Value { get; set; }
        #endregion
    }
}

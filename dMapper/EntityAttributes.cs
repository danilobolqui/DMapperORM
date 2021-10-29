using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace dMapper
{
    public class EntityAttributes
    {
        public class PrimaryKeyAttributes : System.Attribute
        {
            #region Construtor
            public PrimaryKeyAttributes(string nomePrimaryKey, bool identity)
            {
                NomePrimaryKey = nomePrimaryKey;
                Identity = identity;
            }
            #endregion

            #region Propriedades
            public string NomePrimaryKey { get; set; }
            public bool Identity { get; set; }
            #endregion
        }
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace dMapper
{
    public class EntityInterface
    {
        #region Interfaces
        public interface IEntity
        {
            string Mapeador_NomeEntidade();
            string Mapeador_NomeStringConexao();
        }
        #endregion
    }
}

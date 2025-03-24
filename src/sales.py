import polars as pl


class DataFileSale:
    location: str

    def __init__(self, location: str) -> None:
        """Busca os dados no diretório informado.

        Args:
            location (str): Localização do arquivo de vendas para leitura.
        """
        self.location = location
        self._df = self._read_csv()

    def _read_csv(self) -> pl.DataFrame:
        """Carrega o arquivo CSV

        Returns:
            pl.DataFrame: Retorna o Dataframe.
        """
        return pl.read_csv(self.location)

    def _get_total_sale_by_product(self) -> pl.DataFrame:
        """Total de vendas por produto

        Returns:
            pl.DataFrame: Retorna um DataFrame com o total de vendas de cada produto.
        """
        return self._df.group_by("produto").agg(
            pl.col("total_venda").sum().alias("total_vendas")
        )

    def _get_total_spent_per_customer(self) -> pl.DataFrame:
        """Total gasto por cliente

        Returns:
            pl.DataFrame: Retorna um DataFrame com o total gasto de cada cliente.
        """
        return self._df.groupby("cliente").agg(
            pl.col("total_venda").sum().alias("gasto_total")
        )

    def _get_best_selling_product(self) -> pl.DataFrame:
        """Produto mais vendido

        Returns:
            pl.DataFrame: Retorna um DataFrame com o produto mais vendido
        """
        return (
            self._df.groupby("produto")
            .agg(pl.col("quantidade").sum().alias("total_quantidade"))
            .sort("total_quantidade", reverse=True)
            .head(1)
        )


if __name__ == "__main__":
    data_sale = DataFileSale("./dados-vendas.csv")
    data_sale._get_total_sale_by_product()
    data_sale._get_best_selling_product()
    data_sale._get_total_spent_per_customer()

-- Table: public.info_cadastral_entidades

-- DROP TABLE IF EXISTS public.info_cadastral_entidades;

CREATE TABLE IF NOT EXISTS public.info_cadastral_entidades
(
    data character varying COLLATE pg_catalog."default",
    codigo_cadastro_bacen character varying COLLATE pg_catalog."default",
    codigo_sisbacen character varying COLLATE pg_catalog."default",
    codigo_pais_sede character varying COLLATE pg_catalog."default",
    nome_pais_sede character varying COLLATE pg_catalog."default",
    nome_uf_sede character varying COLLATE pg_catalog."default",
    codigo_municipio_sede character varying COLLATE pg_catalog."default",
    nome_municipio_sede character varying COLLATE pg_catalog."default",
    nome_entidade character varying COLLATE pg_catalog."default",
    nome_entidade_nao_formatado character varying COLLATE pg_catalog."default",
    cnpj character varying COLLATE pg_catalog."default",
    cnpj_raiz character varying COLLATE pg_catalog."default",
    codigo_situacao character varying COLLATE pg_catalog."default",
    descricao_situacao character varying COLLATE pg_catalog."default",
    codigo_tipo_entidade_segmento character varying COLLATE pg_catalog."default",
    nome_tipo_entidade character varying COLLATE pg_catalog."default",
    codigo_natureza_juridica character varying COLLATE pg_catalog."default",
    descricao_natureza_juridica character varying COLLATE pg_catalog."default",
    codigo_esfera_publica character varying COLLATE pg_catalog."default",
    nome_reduzido character varying COLLATE pg_catalog."default",
    sigla_entidade character varying COLLATE pg_catalog."default",
    nome_fantasia character varying COLLATE pg_catalog."default",
    empresa_publica character varying COLLATE pg_catalog."default",
    CONSTRAINT unique_constraint UNIQUE (codigo_cadastro_bacen, codigo_sisbacen, codigo_pais_sede, nome_pais_sede, nome_uf_sede, codigo_municipio_sede, nome_municipio_sede, nome_entidade, nome_entidade_nao_formatado, cnpj, cnpj_raiz, codigo_situacao, descricao_situacao, codigo_tipo_entidade_segmento, nome_tipo_entidade, codigo_natureza_juridica, descricao_natureza_juridica, codigo_esfera_publica, nome_reduzido, sigla_entidade, nome_fantasia, empresa_publica)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.info_cadastral_entidades
    OWNER to postgres;
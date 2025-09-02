from typing import List, Dict
from models.multidatabase_model import MultidatabaseModel


class MenuV2Model(MultidatabaseModel):

    def set_bs_menu_items(self):
        resp = {}
        try:
            query = """
                INSERT INTO `bs_menu` 
                    (`m_name`, `m_status`, `m_type`) 
                VALUES
                    ("bs_mnu_documents_v2", 1, NULL),
                    ("bs_mnu_mae_product_v2", 1, NULL),
                    ("bs_mnu_mae_client_v2", 1, NULL),
                    ("bs_mnu_online_store_v2", 1, NULL),
                    ("bs_mnu_online_store_mp_v2", 1, NULL),
                    ("bs_mnu_reports_v2", 1, NULL),
                    ("bs_mnu_settings_v2", 1, NULL),
                    ("bs_mnu_main_v2", 1, NULL);
            """
            r = self._execute_query(query, (), False)
            if not r["success"]:
                raise ValueError(r["error"])

            resp["success"] = True
            resp["row_affected"] = r["row_affected"]
        except ValueError as error:
            resp["error"] = str(error)
            resp["success"] = False
        return resp

    # CREAR TABLA TEMPORAL
    def set_temporary_table(self):
        resp = {}
        try:
            query = """
                CREATE TEMPORARY TABLE bs_temporary (
                    ml_id			    int(11),
                    ml_name			    varchar(250),
                    ml_active		    int(1),
                    ml_asociate	        int(11),
                    ml_url			    varchar(100),
                    m_id			    int(11),
                    ml_is_dropdown	    int(1),
                    id_accion_modulo    int(11),
                    ml_order		    int(11)
                );
            """
            r = self._execute_query(query, (), False)
            if not r["success"]:
                raise ValueError(r["error"])

            resp["success"] = True
            resp["row_affected"] = r["row_affected"]
        except ValueError as error:
            resp["error"] = str(error)
            resp["success"] = False
        return resp

    # CLIENTES
    def set_menu_clients(self):
        resp = {}
        try:
            query = """
                SET @bs_mnu_client_id = (
                    SELECT m_id
                    FROM bs_menu
                    WHERE m_name = 'bs_mnu_mae_client_v2' 
                );

                SET @accion_modulo_maes_clientes_new_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.maes.clientes.new' LIMIT 1);
                SET @accion_modulo_maes_clientes_edit_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.maes.clientes.edit' LIMIT 1);
                SET @accion_modulo_maes_bpoints_catalogo_new_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.maes.bpoints_catalogo.new' LIMIT 1);
                SET @accion_modulo_maes_bpoints_edit_edit_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.maes.bpoints_edit.edit' LIMIT 1);

                INSERT INTO `bs_temporary` 
                    (`ml_name`, `ml_active`, `ml_asociate`, `ml_url`, `m_id`, `ml_is_dropdown`, `id_accion_modulo`, `ml_order`) 
                VALUES 
                    ("bs_mnu_mae_client.clients_new", 1, 0, "/goto?owner=clients&url=/admin/clients", @bs_mnu_client_id, 0, @accion_modulo_maes_clientes_new_id, 1),
                    ("bs_mnu_mae_client.clients_new", 1, 0, "/goto?owner=clients&url=/admin/clients", @bs_mnu_client_id, 0, @accion_modulo_maes_clientes_edit_id, 1),
                    ("bs_mnu_mae_client.attr", 1, 0, "/goto?owner=clients&url=/admin/clients/dynamic_attributes", @bs_mnu_client_id, 0, @accion_modulo_maes_clientes_new_id, 2),
                    ("bs_mnu_mae_client.attr", 1, 0, "/goto?owner=clients&url=/admin/clients/dynamic_attributes", @bs_mnu_client_id, 0, @accion_modulo_maes_clientes_edit_id, 2),
                    ("bs_mnu_mae_client.bpoints_new", 1, 0, "/goto?owner=bsale&url=/admin/bpoints", @bs_mnu_client_id, 0, @accion_modulo_maes_bpoints_catalogo_new_id, 3),
                    ("bs_mnu_mae_client.bpoints_new", 1, 0, "/goto?owner=bsale&url=/admin/bpoints", @bs_mnu_client_id, 0, @accion_modulo_maes_bpoints_edit_edit_id, 3);
            """
            r = self._execute_query(query, (), False, True)
            if not r["success"]:
                raise ValueError(r["error"])

            resp["success"] = True
            resp["row_affected"] = r["row_affected"]
        except ValueError as error:
            resp["error"] = str(error)
            resp["success"] = False
        return resp

    # PRODUCTOS   
    def set_menu_products(self):
        resp = {}
        try:
            query = """
                SET @bs_mnu_mae_product_id = (
                    SELECT m_id
                    FROM bs_menu
                    WHERE m_name = 'bs_mnu_mae_product_v2'
                );

                -- MENU "PRODUCTOS"
                INSERT INTO `bs_menu_link` 
                    (`ml_name`, `ml_active`, `ml_asociate`, `ml_url`, `m_id`, `ml_is_dropdown`, `id_accion_modulo`, `ml_order`)
                VALUES 
                    ("bs_mnu_mae_product.products", 1, 0, NULL, @bs_mnu_mae_product_id, 1, 0, 2);
                
                SET @bs_mnu_products_id = (
                    SELECT ml_id
                    FROM bs_menu_link
                    WHERE ml_name = 'bs_mnu_mae_product.products'
                    AND m_id = @bs_mnu_mae_product_id
                );

                SET @accion_modulo_maes_products_new_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.maes.productos.new' LIMIT 1);
                SET @accion_modulo_maes_products_edit_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.maes.productos.edit' LIMIT 1);
                SET @accion_modulo_maes_tipo_product_new_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.maes.tipo_producto.new' LIMIT 1);
                SET @accion_modulo_maes_tipo_product_edit_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.maes.tipo_producto.edit' LIMIT 1);
                SET @accion_modulo_pos_online_store_new_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.pos.online_store.new' LIMIT 1);
                SET @accion_modulo_maes_giftcards_new_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.maes.giftcards.new' LIMIT 1);
                SET @accion_modulo_maes_giftcards_edit_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.maes.giftcards.edit' LIMIT 1);
                SET @bs_mnu_online_store_status = (SELECT ml_active FROM bs_menu_link WHERE ml_name = 'bs_mnu_main.online_store' LIMIT 1);
                SET @giftcard_sys_config = (SELECT valor FROM sys_config WHERE variable = 'giftcard_module' LIMIT 1);
                SET @accion_modulo_maes_kit_new_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.maes.plan.new' LIMIT 1);
                SET @accion_modulo_maes_kit_edit_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.maes.plan.edit' LIMIT 1);

                INSERT INTO `bs_temporary` 
                    (`ml_name`, `ml_active`, `ml_asociate`, `ml_url`, `m_id`, `ml_is_dropdown`, `id_accion_modulo`, `ml_order`) 
                VALUES
                    ("bs_mnu_mae_product.my_product", 1, @bs_mnu_products_id, "/goto?owner=product_admin&url=/admin/products", @bs_mnu_mae_product_id, 0, @accion_modulo_maes_products_new_id, 1),
                    ("bs_mnu_mae_product.my_product", 1, @bs_mnu_products_id, "/goto?owner=product_admin&url=/admin/products", @bs_mnu_mae_product_id, 0, @accion_modulo_maes_products_edit_id, 1),
                    ("bs_mnu_mae_product.my_product_type", 1, @bs_mnu_products_id, "/goto?owner=product_admin&url=/admin/categories", @bs_mnu_mae_product_id, 0, @accion_modulo_maes_tipo_product_new_id, 2),
                    ("bs_mnu_mae_product.my_product_type", 1, @bs_mnu_products_id, "/goto?owner=product_admin&url=/admin/categories", @bs_mnu_mae_product_id, 0, @accion_modulo_maes_tipo_product_edit_id, 2),
                    ("bs_mnu_mae_product.collection", @bs_mnu_online_store_status, @bs_mnu_products_id, "/goto?owner=market_admin&url=/admin/config/collections", @bs_mnu_mae_product_id, 0, @accion_modulo_pos_online_store_new_id, 3),
                    ("bs_mnu_mae_product.brand", 1, @bs_mnu_products_id, "/goto?owner=product_new&url=/admin/v2/brands", @bs_mnu_mae_product_id, 0, @accion_modulo_maes_products_new_id, 4),
                    ("bs_mnu_mae_product.brand", 1, @bs_mnu_products_id, "/goto?owner=product_new&url=/admin/v2/brands", @bs_mnu_mae_product_id, 0, @accion_modulo_maes_products_edit_id, 4),	
                    ("bs_mnu_mae_product.kit", 1, @bs_mnu_products_id, "/goto?owner=bsale&url=/admin/kits", @bs_mnu_mae_product_id, 0, @accion_modulo_maes_kit_new_id, 5),
                    ("bs_mnu_mae_product.kit", 1, @bs_mnu_products_id, "/goto?owner=bsale&url=/admin/kits", @bs_mnu_mae_product_id, 0, @accion_modulo_maes_kit_edit_id, 5),
                    ("bs_mnu_mae_product.giftcard", @giftcard_sys_config, @bs_mnu_products_id, "/goto?owner=loyalty_v2&url=/admin/giftcard", @bs_mnu_mae_product_id, 0, @accion_modulo_maes_giftcards_new_id, 6),
                    ("bs_mnu_mae_product.giftcard", @giftcard_sys_config, @bs_mnu_products_id, "/goto?owner=loyalty_v2&url=/admin/giftcard", @bs_mnu_mae_product_id, 0, @accion_modulo_maes_giftcards_edit_id, 6),
                    ("bs_mnu_mae_product.config", 1, @bs_mnu_products_id, "/goto?owner=bsale&url=/admin/config/products", @bs_mnu_mae_product_id, 0, @accion_modulo_maes_products_edit_id, 7);

                -- MENU "LISTAS DE PRECIOS"
                SET @accion_modulo_maes_lista_precios_new_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.maes.lista_precios.new' LIMIT 1);
                SET @accion_modulo_maes_lista_precios_edit_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.maes.lista_precios.edit' LIMIT 1);

                INSERT INTO `bs_temporary` 
                    (`ml_name`, `ml_active`, `ml_asociate`, `ml_url`, `m_id`, `ml_is_dropdown`, `id_accion_modulo`, `ml_order`)
                VALUES 
                    ("bs_mnu_mae_product.price_list", 1, 0, '/goto?owner=bsale&url=/admin/lista_precios', @bs_mnu_mae_product_id, 0, @accion_modulo_maes_lista_precios_new_id, 3),
                    ("bs_mnu_mae_product.price_list", 1, 0, '/goto?owner=bsale&url=/admin/lista_precios', @bs_mnu_mae_product_id, 0, @accion_modulo_maes_lista_precios_edit_id, 3);

                -- MENU "STOCK"
                -- SET @mnu_stock_status = (SELECT ml_active FROM bs_menu_link WHERE ml_name = 'bs_mnu_main.stock' LIMIT 1);
                SET @sys_stock_plan_active = (SELECT valor FROM sys_config WHERE variable = 'stock_plan_active' LIMIT 1);
                SET @sys_categoria_producto = (SELECT valor FROM sys_config WHERE variable = 'categoria_producto' LIMIT 1);
                SET @status_acc_mod_maes_stock_new_id = (SELECT estado_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.maes.stock.new' LIMIT 1);
                SET @status_acc_mod_maes_stock_edit_id = (SELECT estado_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.maes.stock.edit' LIMIT 1);
                SET @mnu_stock_status_evaluated = ( (@status_acc_mod_maes_stock_new_id = 0 OR @status_acc_mod_maes_stock_edit_id = 0) AND @sys_stock_plan_active AND @sys_categoria_producto <> 1);
                INSERT INTO `bs_menu_link` 
                    (`ml_name`, `ml_active`, `ml_asociate`, `ml_url`, `m_id`, `ml_is_dropdown`, `id_accion_modulo`, `ml_order`)
                VALUES 
                    ("bs_mnu_mae_product.stock", @mnu_stock_status_evaluated, 0, NULL, @bs_mnu_mae_product_id, 1, 0, 4);
                    
                SET @bs_mnu_stock_id = (
                    SELECT ml_id
                    FROM bs_menu_link
                    WHERE ml_name = 'bs_mnu_mae_product.stock'
                    AND m_id = @bs_mnu_mae_product_id
                );

                SET @accion_modulo_reports_stock_view_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.reports.stock.view' LIMIT 1);
                SET @accion_modulo_maes_stock_new_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.maes.stock.new' LIMIT 1);
                SET @accion_modulo_maes_stock_edit_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.maes.stock.edit' LIMIT 1);
                SET @accion_modulo_maes_inventario_new_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.maes.inventario.new' LIMIT 1);
                SET @accion_modulo_pos_update_costos_new_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.pos.update_costos.new' LIMIT 1);
                SET @mnu_stock_inventory_status = (SELECT ml_active FROM bs_menu_link WHERE ml_name = 'bs_mnu_stock.inventory' LIMIT 1);

                SET @use_stock_v2 = (
                    SELECT COALESCE(MAX(valor), 0) 
                    FROM sys_config 
                    WHERE variable = 'use_stock_v2' AND valor = 1
                );

                SET @stock_actual_url = (
                    SELECT 
                        CASE 
                            WHEN @use_stock_v2 = 1 THEN '/goto?owner=stock_v2&url=/reports/stock/'
                            ELSE '/goto?owner=bsale&url=/reports/stock/'
                        END
                );

                SET @recepcion_url = (
                    SELECT 
                        CASE 
                            WHEN @use_stock_v2 = 1 THEN '/goto?owner=stock_v2&url=/admin/stock/reception'
                            ELSE '/goto?owner=bsale&url=/admin/stock/add_stock'
                        END
                );
                
                SET @consumo_url = (
                    SELECT 
                        CASE 
                            WHEN @use_stock_v2 = 1 THEN '/goto?owner=stock_v2&url=/admin/stock/consumption'
                            ELSE '/goto?owner=bsale&url=/admin/stock/consumption'
                        END
                );

                INSERT INTO `bs_temporary` 
                    (`ml_name`, `ml_active`, `ml_asociate`, `ml_url`, `m_id`, `ml_is_dropdown`, `id_accion_modulo`, `ml_order`)
                VALUES
                    ("bs_mnu_mae_product.current_stock", 1, @bs_mnu_stock_id , @stock_actual_url, @bs_mnu_mae_product_id, 0, @accion_modulo_reports_stock_view_id, 1),
                    ("bs_mnu_mae_product.days_of_stock", 1, @bs_mnu_stock_id , '/goto?owner=bsale&url=/admin/stock/critic', @bs_mnu_mae_product_id, 0, @accion_modulo_maes_stock_edit_id, 2),
                    ("bs_mnu_mae_product.import", 1, @bs_mnu_stock_id , '/goto?owner=bsale&url=/admin/imports/stock', @bs_mnu_mae_product_id, 0, @accion_modulo_maes_stock_new_id, 3),
                    ("bs_mnu_mae_product.reception", 1, @bs_mnu_stock_id , @recepcion_url, @bs_mnu_mae_product_id, 0, @accion_modulo_maes_stock_new_id, 4),
                    ("bs_mnu_mae_product.consumption", 1, @bs_mnu_stock_id , @consumo_url, @bs_mnu_mae_product_id, 0, @accion_modulo_maes_stock_edit_id, 5),
                    ("bs_mnu_mae_product.inventory", @mnu_stock_inventory_status, @bs_mnu_stock_id , '/goto?owner=bsale&url=/admin/stock/inventory', @bs_mnu_mae_product_id, 0, @accion_modulo_maes_inventario_new_id, 6),
                    ("bs_mnu_mae_product.cost_update", 1, @bs_mnu_stock_id , '/goto?owner=bsale&url=/admin/stock/cost_update', @bs_mnu_mae_product_id, 0, @accion_modulo_pos_update_costos_new_id, 7),
                    ("bs_mnu_mae_product.update_stock", 1, @bs_mnu_stock_id , '/goto?owner=bsale&url=/admin/stock/update_stock', @bs_mnu_mae_product_id, 0, @accion_modulo_maes_stock_edit_id, 8),
                    ("bs_mnu_mae_product.edit_reception", 1, @bs_mnu_stock_id , '/goto?owner=bsale&url=/admin/stock/edit_reception', @bs_mnu_mae_product_id, 0, @accion_modulo_maes_stock_edit_id, 9),
                    ("bs_mnu_mae_product.massive_cost", 1, @bs_mnu_stock_id , '/goto?owner=bsale&url=/admin/imports/cost', @bs_mnu_mae_product_id, 0, @accion_modulo_pos_update_costos_new_id, 10);

                -- MENU "PROMOCIONES"
                INSERT INTO `bs_menu_link` 
                    (`ml_name`, `ml_active`, `ml_asociate`, `ml_url`, `m_id`, `ml_is_dropdown`, `id_accion_modulo`, `ml_order`)
                VALUES 
                    ("bs_mnu_mae_product.promotions", 1, 0, NULL, @bs_mnu_mae_product_id, 1, 0, 5);
                    
                SET @bs_mnu_promotions_id = (
                    SELECT ml_id
                    FROM bs_menu_link
                    WHERE ml_name = 'bs_mnu_mae_product.promotions'
                    AND m_id = @bs_mnu_mae_product_id
                );

                SET @accion_modulo_maes_descuentos_new_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.maes.descuentos.new' LIMIT 1);
                SET @accion_modulo_maes_descuentos_edit_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.maes.descuentos.edit' LIMIT 1);
                SET @accion_modulo_maes_cupones_new_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.maes.cupones.new' LIMIT 1);
                SET @accion_modulo_maes_cupones_edit_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.maes.cupones.edit' LIMIT 1);

                INSERT INTO `bs_temporary` 
                    (`ml_name`, `ml_active`, `ml_asociate`, `ml_url`, `m_id`, `ml_is_dropdown`, `id_accion_modulo`, `ml_order`) 
                VALUES 
                    ("bs_mnu_mae_product.discount", 1, @bs_mnu_promotions_id, "/goto?owner=discount&url=/admin/discounts", @bs_mnu_mae_product_id, 0, @accion_modulo_maes_descuentos_new_id, 1),
                    ("bs_mnu_mae_product.discount", 1, @bs_mnu_promotions_id, "/goto?owner=discount&url=/admin/discounts", @bs_mnu_mae_product_id, 0, @accion_modulo_maes_descuentos_edit_id, 1),
                    ("bs_mnu_mae_product.coupons", 1, @bs_mnu_promotions_id, "/goto?owner=discount&url=/admin/coupons", @bs_mnu_mae_product_id, 0, @accion_modulo_maes_cupones_new_id, 2),
                    ("bs_mnu_mae_product.coupons", 1, @bs_mnu_promotions_id, "/goto?owner=discount&url=/admin/coupons", @bs_mnu_mae_product_id, 0, @accion_modulo_maes_cupones_edit_id, 2);
    

                -- MENU "ACCESOS DIRECTOS"
                INSERT INTO `bs_menu_link` 
                    (`ml_name`, `ml_active`, `ml_asociate`, `ml_url`, `m_id`, `ml_is_dropdown`, `id_accion_modulo`, `ml_order`)
                VALUES 
                    ("bs_mnu_mae_product.shortcuts", 1, 0, NULL, @bs_mnu_mae_product_id, 1, 0, 1);
                    
                SET @bs_mnu_product_shortcuts_id = (
                    SELECT ml_id
                    FROM bs_menu_link
                    WHERE ml_name = 'bs_mnu_mae_product.shortcuts'
                    AND m_id = @bs_mnu_mae_product_id
                );

                INSERT INTO `bs_temporary` 
                    (`ml_name`, `ml_active`, `ml_asociate`, `ml_url`, `m_id`, `ml_is_dropdown`, `id_accion_modulo`, `ml_order`)
                VALUES 
                    ("bs_mnu_mae_product.mnu_shortcuts.inventory", @mnu_stock_status_evaluated, @bs_mnu_product_shortcuts_id , '/goto?owner=bsale&url=/admin/stock/inventory', @bs_mnu_mae_product_id, 0, @accion_modulo_maes_inventario_new_id, 1),
                    ("bs_mnu_mae_product.mnu_shortcuts.consumption", @mnu_stock_status_evaluated, @bs_mnu_product_shortcuts_id , @consumo_url, @bs_mnu_mae_product_id, 0, @accion_modulo_maes_stock_edit_id, 2),
                    ("bs_mnu_mae_product.mnu_shortcuts.reception", @mnu_stock_status_evaluated, @bs_mnu_product_shortcuts_id , @recepcion_url, @bs_mnu_mae_product_id, 0, @accion_modulo_maes_stock_new_id, 3),
                    ("bs_mnu_mae_product.mnu_shortcuts.create_product", 1, @bs_mnu_product_shortcuts_id , NULL, @bs_mnu_mae_product_id, 0, @accion_modulo_maes_products_new_id, 4),
                    ("bs_mnu_mae_product.mnu_shortcuts.stock_card", @mnu_stock_status_evaluated, @bs_mnu_product_shortcuts_id , '/goto?owner=bsale&url=/reports/stock/card', @bs_mnu_mae_product_id, 0, @accion_modulo_reports_stock_view_id, 5),
                    ("bs_mnu_mae_product.mnu_shortcuts.current_stock", @mnu_stock_status_evaluated, @bs_mnu_product_shortcuts_id , @stock_actual_url, @bs_mnu_mae_product_id, 0, @accion_modulo_reports_stock_view_id, 6),
                    ("bs_mnu_mae_product.mnu_shortcuts.my_product", 1, @bs_mnu_product_shortcuts_id, "/goto?owner=product_admin&url=/admin/products", @bs_mnu_mae_product_id, 0, @accion_modulo_maes_products_new_id, 7),
                    ("bs_mnu_mae_product.mnu_shortcuts.my_product", 1, @bs_mnu_product_shortcuts_id, "/goto?owner=product_admin&url=/admin/products", @bs_mnu_mae_product_id, 0, @accion_modulo_maes_products_edit_id, 7),
                    ("bs_mnu_mae_product.mnu_shortcuts.cost_update", @mnu_stock_status_evaluated, @bs_mnu_product_shortcuts_id , '/goto?owner=bsale&url=/admin/stock/cost_update', @bs_mnu_mae_product_id, 0, @accion_modulo_pos_update_costos_new_id, 8),
                    ("bs_mnu_mae_product.mnu_shortcuts.update_stock", @mnu_stock_status_evaluated, @bs_mnu_product_shortcuts_id , '/goto?owner=bsale&url=/admin/stock/update_stock', @bs_mnu_mae_product_id, 0, @accion_modulo_maes_stock_edit_id, 9),
                    ("bs_mnu_mae_product.mnu_shortcuts.mnu_create_product.create_product", 1, @bs_mnu_product_shortcuts_id, '/goto?owner=product_new&url=/admin/v2/products?action=create_product', @bs_mnu_mae_product_id, 0, @accion_modulo_maes_products_new_id, 10),
                    ("bs_mnu_mae_product.mnu_shortcuts.mnu_create_product.create_service", 1, @bs_mnu_product_shortcuts_id, '/goto?owner=product_new&url=/admin/v2/products?action=create_service', @bs_mnu_mae_product_id, 0, @accion_modulo_maes_products_new_id, 11),
                    ("bs_mnu_mae_product.mnu_shortcuts.mnu_create_product.create_pack", 1, @bs_mnu_product_shortcuts_id, '/goto?owner=product_new&url=/admin/v2/products?action=create_pack', @bs_mnu_mae_product_id, 0, @accion_modulo_maes_products_new_id, 12);
                """
            r = self._execute_query(query, (), False, True)
            if not r["success"]:
                raise ValueError(r["error"])

            resp["success"] = True
            resp["row_affected"] = r["row_affected"]
        except ValueError as error:
            resp["error"] = str(error)
            resp["success"] = False
        return resp

    # DOCUMENTOS   
    def set_menu_documents(self):
        resp = {}
        try:
            query = """
                SET @bs_mnu_documents_id = (
                    SELECT m_id
                    FROM bs_menu
                    WHERE m_name = 'bs_mnu_documents_v2'
                );

                INSERT INTO `bs_menu_link` 
                    (`ml_name`, `ml_active`, `ml_asociate`, `ml_url`, `m_id`, `ml_is_dropdown`, `id_accion_modulo`, `ml_order`)
                VALUES 
                    ("bs_mnu_documents.new", 1, 0, NULL, @bs_mnu_documents_id, 1, 0, 1);
                    
                SET @bs_mnu_documents_new_id = (
                    SELECT ml_id
                    FROM bs_menu_link
                    WHERE ml_name = 'bs_mnu_documents.new'
                    AND m_id = @bs_mnu_documents_id
                );

                SET @accion_modulo_pos_venta_new_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.pos.venta.new' LIMIT 1);
                SET @accion_modulo_pos_genera_documentos_new_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.pos.genera_documentos.new' LIMIT 1);
                SET @full_import_status = (SELECT ml_active FROM bs_menu_link WHERE ml_name = 'bs_mnu_documents.mnu_new.full_import' LIMIT 1);
                SET @purchase_status = (SELECT ml_active FROM bs_menu_link WHERE ml_name = 'bs_mnu_documents.mnu_new.purchase' LIMIT 1);

                INSERT INTO `bs_temporary` 
                    (`ml_name`, `ml_active`, `ml_asociate`, `ml_url`, `m_id`, `ml_is_dropdown`, `id_accion_modulo`, `ml_order`)
                VALUES 
                    ("bs_mnu_documents.mnu_new.blank", 1, @bs_mnu_documents_new_id , '/goto?owner=bsale&url=/documents/sales', @bs_mnu_documents_id, 0, @accion_modulo_pos_venta_new_id, 1),
                    ("bs_mnu_documents.mnu_new.from_existing", 1, @bs_mnu_documents_new_id , '/goto?owner=bsale&url=/documents/from_existing', @bs_mnu_documents_id, 0, @accion_modulo_pos_genera_documentos_new_id, 2),
                    ("bs_mnu_documents.mnu_new.detail_import", 1, @bs_mnu_documents_new_id , '/goto?owner=bsale&url=/documents/detail_import', @bs_mnu_documents_id, 0, @accion_modulo_pos_venta_new_id, 3),
                    ("bs_mnu_documents.mnu_new.full_import", @full_import_status, @bs_mnu_documents_new_id , '/goto?owner=bsale&url=/admin/imports/documents', @bs_mnu_documents_id, 0, @accion_modulo_pos_venta_new_id, 4),
                    ("bs_mnu_documents.mnu_new.purchase", @purchase_status, @bs_mnu_documents_new_id , '/goto?owner=bsale&url=/documents/purchase', @bs_mnu_documents_id, 0, @accion_modulo_pos_genera_documentos_new_id, 5);

                INSERT INTO `bs_temporary` 
                    (`ml_name`, `ml_active`, `ml_asociate`, `ml_url`, `m_id`, `ml_is_dropdown`, `id_accion_modulo`, `ml_order`)
                SELECT 
                    "bs_mnu_documents.mnu_new.cfdi_global", 1, @bs_mnu_documents_new_id, '/goto?owner=dte_v2&url=/cfdi_global_list', 
                    @bs_mnu_documents_id, 0, @accion_modulo_pos_genera_documentos_new_id, 6
                FROM DUAL
                WHERE EXISTS (
                    SELECT 1
                    FROM bs_menu_link
                    WHERE ml_name = 'bs_mnu_documents.mnu_more.cfdi_global'
                    LIMIT 1
                );
                
                -- REDIRECCIÓN "BUSCAR/ENVIAR" - "PEDIDOS WEB"
                SET @accion_modulo_pos_venta_edit_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.pos.venta.edit' LIMIT 1);
                SET @accion_modulo_pos_venta_pdv_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.pos.venta.pdv' LIMIT 1);
                SET @accion_modulo_pos_online_orders_new_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.pos.online_orders.new' LIMIT 1);
                SET @orders_status = (SELECT ml_active FROM bs_menu_link WHERE ml_name = 'bs_mnu_documents.online_orders' LIMIT 1);

                INSERT INTO `bs_temporary` 
                    (`ml_name`, `ml_active`, `ml_asociate`, `ml_url`, `m_id`, `ml_is_dropdown`, `id_accion_modulo`, `ml_order`)
                VALUES 
                    ("bs_mnu_documents.search", 1, 0, '/goto?owner=bsale&url=/documents/search', @bs_mnu_documents_id, 0, @accion_modulo_pos_venta_new_id, 2),
                    ("bs_mnu_documents.search", 1, 0, '/goto?owner=bsale&url=/documents/search', @bs_mnu_documents_id, 0, @accion_modulo_pos_venta_edit_id, 2),
                    ("bs_mnu_documents.search", 1, 0, '/goto?owner=bsale&url=/documents/search', @bs_mnu_documents_id, 0, @accion_modulo_pos_venta_pdv_id, 2),
                    ("bs_mnu_documents.search", 1, 0, '/goto?owner=bsale&url=/documents/search', @bs_mnu_documents_id, 0, @accion_modulo_pos_genera_documentos_new_id, 2),
                    ("bs_mnu_documents.online_orders", @orders_status, 0, '/goto?owner=document&url=/orders/status/', @bs_mnu_documents_id, 0, @accion_modulo_pos_online_orders_new_id, 3);
                
                -- MENU "DESPACHO"
                INSERT INTO `bs_menu_link` 
                    (`ml_name`, `ml_active`, `ml_asociate`, `ml_url`, `m_id`, `ml_is_dropdown`, `id_accion_modulo`, `ml_order`)
                VALUES 
                    ("bs_mnu_documents.shipping", 1, 0, NULL, @bs_mnu_documents_id, 1, 0, 4);
                        
                SET @bs_mnu_documents_shipping_id = (
                    SELECT ml_id
                    FROM bs_menu_link
                    WHERE ml_name = 'bs_mnu_documents.shipping'
                    AND m_id = @bs_mnu_documents_id
                );

                SET @accion_modulo_pos_despacho_new_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.pos.despacho.new' LIMIT 1);
                SET @accion_modulo_pos_despacho_edit_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.pos.despacho.edit' LIMIT 1);

                INSERT INTO `bs_temporary` 
                    (`ml_name`, `ml_active`, `ml_asociate`, `ml_url`, `m_id`, `ml_is_dropdown`, `id_accion_modulo`, `ml_order`)
                VALUES 
                    ("bs_mnu_documents.mnu_shipping.blank", 1, @bs_mnu_documents_shipping_id, '/goto?owner=bsale&url=/documents/shipping/from_scratch', @bs_mnu_documents_id, 0, @accion_modulo_pos_despacho_new_id, 1),
                    ("bs_mnu_documents.mnu_shipping.from_existing", 1, @bs_mnu_documents_shipping_id, '/goto?owner=bsale&url=/documents/shipping', @bs_mnu_documents_id, 0, @accion_modulo_pos_despacho_new_id, 2),
                    ("bs_mnu_documents.mnu_shipping.import_detail", 1, @bs_mnu_documents_shipping_id, '/goto?owner=bsale&url=/documents/detail_import', @bs_mnu_documents_id, 0, @accion_modulo_pos_despacho_new_id, 3),
                    ("bs_mnu_documents.mnu_shipping.annul", 1, @bs_mnu_documents_shipping_id, '/goto?owner=bsale&url=/documents/shipping/annul', @bs_mnu_documents_id, 0, @accion_modulo_pos_despacho_edit_id, 4);


                -- MENU "DEVOLUCIONES"
                INSERT INTO `bs_menu_link` 
                    (`ml_name`, `ml_active`, `ml_asociate`, `ml_url`, `m_id`, `ml_is_dropdown`, `id_accion_modulo`, `ml_order`)
                VALUES 
                    ("bs_mnu_documents.return", 1, 0, NULL, @bs_mnu_documents_id, 1, 0, 5);
                        
                SET @bs_mnu_documents_return_id = (
                    SELECT ml_id
                    FROM bs_menu_link
                    WHERE ml_name = 'bs_mnu_documents.return'
                    AND m_id = @bs_mnu_documents_id
                );

                SET @accion_modulo_pos_devoluciones_new_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.pos.devoluciones.new' LIMIT 1);
                SET @accion_modulo_pos_devoluciones_edit_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.pos.devoluciones.edit' LIMIT 1);
                SET @accion_modulo_pos_devoluciones_monto_new_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.pos.devoluciones_monto.new' LIMIT 1);
                SET @product_status = (SELECT ml_active FROM bs_menu_link WHERE ml_name = 'bs_mnu_documents.mnu_return.product' LIMIT 1);
                SET @text_status = (SELECT ml_active FROM bs_menu_link WHERE ml_name = 'bs_mnu_documents.mnu_return.text' LIMIT 1);
                SET @price_adjustment_down_status = (SELECT ml_active FROM bs_menu_link WHERE ml_name = 'bs_mnu_documents.mnu_return.price_adjustment_down' LIMIT 1);
                SET @price_adjustment_up_status = (SELECT ml_active FROM bs_menu_link WHERE ml_name = 'bs_mnu_documents.mnu_return.price_adjustment_up' LIMIT 1);
                SET @liquidation_status = (SELECT ml_active FROM bs_menu_link WHERE ml_name = 'bs_mnu_documents.mnu_return.liquidation' LIMIT 1);
                SET @annul_status = (SELECT ml_active FROM bs_menu_link WHERE ml_name = 'bs_mnu_documents.mnu_return.annul' LIMIT 1);
                SET @price_adjustment_annul_status = (SELECT ml_active FROM bs_menu_link WHERE ml_name = 'bs_mnu_documents.mnu_return.price_adjustment_annul' LIMIT 1);
                SET @purchase_annul_status = (SELECT ml_active FROM bs_menu_link WHERE ml_name = 'bs_mnu_documents.mnu_return.purchase_annul' LIMIT 1);
                SET @void_status = (SELECT ml_active FROM bs_menu_link WHERE ml_name = 'bs_mnu_documents.mnu_return.void' LIMIT 1);

                INSERT INTO `bs_temporary`
                    (`ml_name`, `ml_active`, `ml_asociate`, `ml_url`, `m_id`, `ml_is_dropdown`, `id_accion_modulo`, `ml_order`)
                VALUES 
                    ("bs_mnu_documents.mnu_return.product", @product_status, @bs_mnu_documents_return_id, '/goto?owner=bsale&url=/documents/returns', @bs_mnu_documents_id, 0, @accion_modulo_pos_devoluciones_new_id, 1),
                    ("bs_mnu_documents.mnu_return.text", @text_status, @bs_mnu_documents_return_id, '/goto?owner=bsale&url=/documents/returns/text_adjustment', @bs_mnu_documents_id, 0, @accion_modulo_pos_devoluciones_new_id, 2),
                    ("bs_mnu_documents.mnu_return.price_adjustment_down", @price_adjustment_down_status, @bs_mnu_documents_return_id, '/goto?owner=bsale&url=/documents/returns/price_adjustment', @bs_mnu_documents_id, 0, @accion_modulo_pos_devoluciones_monto_new_id, 3),
                    ("bs_mnu_documents.mnu_return.price_adjustment_up", @price_adjustment_up_status, @bs_mnu_documents_return_id, '/goto?owner=bsale&url=/documents/price_adjustment', @bs_mnu_documents_id, 0, @accion_modulo_pos_devoluciones_edit_id, 4),
                    ("bs_mnu_documents.mnu_return.liquidation", @liquidation_status, @bs_mnu_documents_return_id, '/goto?owner=bsale&url=/documents/returns/liquidation', @bs_mnu_documents_id, 0, @accion_modulo_pos_genera_documentos_new_id, 5),
                    ("bs_mnu_documents.mnu_return.annul", @annul_status, @bs_mnu_documents_return_id, '/goto?owner=bsale&url=/documents/returns/annul', @bs_mnu_documents_id, 0, @accion_modulo_pos_devoluciones_edit_id, 6),
                    ("bs_mnu_documents.mnu_return.price_adjustment_annul", @price_adjustment_annul_status, @bs_mnu_documents_return_id, '/goto?owner=bsale&url=/documents/price_adjustment/annul', @bs_mnu_documents_id, 0, @accion_modulo_pos_devoluciones_edit_id, 7),
                    ("bs_mnu_documents.mnu_return.purchase_annul", @purchase_annul_status, @bs_mnu_documents_return_id, '/goto?owner=bsale&url=/documents/purchase/annul', @bs_mnu_documents_id, 0, @accion_modulo_pos_genera_documentos_new_id, 8);

                -- DAR DE BAJA A NOTA DE CRÉDITO 
                INSERT INTO `bs_temporary` 
                    (`ml_name`, `ml_active`, `ml_asociate`, `ml_url`, `m_id`, `ml_is_dropdown`, `id_accion_modulo`, `ml_order`)
                SELECT 
                    "bs_mnu_documents.mnu_return.void", @void_status, @bs_mnu_documents_return_id, '/goto?owner=bsale&url=/documents/returns/void', @bs_mnu_documents_id, 0, @accion_modulo_pos_devoluciones_edit_id, 9
                FROM DUAL
                WHERE EXISTS (
                    SELECT 1
                    FROM bs_menu_link
                    WHERE ml_name = 'bs_mnu_documents.mnu_return.void'
                    LIMIT 1
                );

                -- REDIRECCIÓN "CRÉDITO"
                SET @accion_modulo_pos_pago_creditos_new_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.pos.pago_creditos.new' LIMIT 1);
                SET @accion_modulo_pos_pago_creditos_edit_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.pos.pago_creditos.edit' LIMIT 1);

                INSERT INTO `bs_temporary` 
                    (`ml_name`, `ml_active`, `ml_asociate`, `ml_url`, `m_id`, `ml_is_dropdown`, `id_accion_modulo`, `ml_order`)
                VALUES 
                    ("bs_mnu_documents.credit", 1, 0, '/goto?owner=bsale&url=/documents/credit', @bs_mnu_documents_id, 0, @accion_modulo_pos_pago_creditos_new_id, 6),
                    ("bs_mnu_documents.credit", 1, 0, '/goto?owner=bsale&url=/documents/credit', @bs_mnu_documents_id, 0, @accion_modulo_pos_pago_creditos_edit_id, 6);

                -- MENU "MAS"
                INSERT INTO `bs_menu_link` 
                    (`ml_name`, `ml_active`, `ml_asociate`, `ml_url`, `m_id`, `ml_is_dropdown`, `id_accion_modulo`, `ml_order`)
                VALUES 
                    ("bs_mnu_documents.more", 1, 0, NULL, @bs_mnu_documents_id, 1, 0, 7);
                        
                SET @bs_mnu_documents_more_id = (
                    SELECT ml_id
                    FROM bs_menu_link
                    WHERE ml_name = 'bs_mnu_documents.more'
                    AND m_id = @bs_mnu_documents_id
                );

                SET @accion_modulo_pos_pago_cierre_mes_new_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.pos.cierre_mes.new' LIMIT 1);
                SET @accion_modulo_pos_pago_cierre_mes_edit_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.pos.cierre_mes.edit' LIMIT 1);
                SET @close_register_status = (SELECT ml_active FROM bs_menu_link WHERE ml_name = 'bs_mnu_documents.mnu_more.close_register' LIMIT 1);
                SET @edit_document_status = (SELECT ml_active FROM bs_menu_link WHERE ml_name = 'bs_mnu_documents.mnu_more.edit_document' LIMIT 1);
                SET @accion_modulo_pos_libro_mensual_new_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.pos.libro_mensual.new' LIMIT 1);
                SET @from_third_party_status = (SELECT ml_active FROM bs_menu_link WHERE ml_name = 'bs_mnu_documents.mnu_more.from_third_party' LIMIT 1);
                SET @new_book_status = (SELECT ml_active FROM bs_menu_link WHERE ml_name = 'bs_mnu_documents.mnu_more.new_book' LIMIT 1);
                SET @import_book_status = (SELECT ml_active FROM bs_menu_link WHERE ml_name = 'bs_mnu_documents.mnu_more.import_book' LIMIT 1);
                SET @import_book_url = (SELECT ml_url FROM bs_menu_link WHERE ml_name = 'bs_mnu_documents.mnu_more.import_book' LIMIT 1);
                SET @calendar_status = (SELECT ml_active FROM bs_menu_link WHERE ml_name = 'bs_mnu_documents.mnu_more.calendar' LIMIT 1);
                SET @milestone_status = (SELECT ml_active FROM bs_menu_link WHERE ml_name = 'bs_mnu_documents.mnu_more.milestone' LIMIT 1);
                SET @summary_documents_status = (SELECT ml_active FROM bs_menu_link WHERE ml_name = 'bs_mnu_documents.mnu_more.summary_documents' LIMIT 1);
                SET @accion_modulo_pos_retiro_abono_cliente_new_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.pos.retiro_abono_cliente.new' LIMIT 1);
                SET @withdrawal_status = (SELECT ml_active FROM bs_menu_link WHERE ml_name = 'bs_mnu_documents.mnu_more.withdrawal' LIMIT 1);
                SET @accion_modulo_pos_cession_dte_new_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.pos.cesion_dte.new' LIMIT 1);
                SET @dte_cession_status = (SELECT ml_active FROM bs_menu_link WHERE ml_name = 'bs_mnu_documents.mnu_more.dte_cession' LIMIT 1);
                SET @accion_modulo_pos_abono_cliente_new_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.pos.abono_cliente.new' LIMIT 1);
                SET @client_income_status = (SELECT ml_active FROM bs_menu_link WHERE ml_name = 'bs_mnu_documents.mnu_more.client_income' LIMIT 1);

                INSERT INTO `bs_temporary` 
                    (`ml_name`, `ml_active`, `ml_asociate`, `ml_url`, `m_id`, `ml_is_dropdown`, `id_accion_modulo`, `ml_order`)
                VALUES 
                    ("bs_mnu_documents.mnu_more.close_register", @close_register_status, @bs_mnu_documents_more_id, '/goto?owner=bsale&url=/close_register', @bs_mnu_documents_id, 0, @accion_modulo_pos_pago_cierre_mes_new_id, 1),
                    ("bs_mnu_documents.mnu_more.close_register", @close_register_status, @bs_mnu_documents_more_id, '/goto?owner=bsale&url=/close_register', @bs_mnu_documents_id, 0, @accion_modulo_pos_pago_cierre_mes_edit_id , 1),
                    ("bs_mnu_documents.mnu_more.edit_document", @edit_document_status, @bs_mnu_documents_more_id, '/goto?owner=bsale&url=/documents/sales/edit', @bs_mnu_documents_id, 0, @accion_modulo_pos_venta_edit_id, 2),
                    ("bs_mnu_documents.mnu_more.from_third_party", @from_third_party_status, @bs_mnu_documents_more_id, '/goto?owner=bsale&url=/documents/search/from_third_party', @bs_mnu_documents_id, 0, @accion_modulo_pos_libro_mensual_new_id, 3),
                    ("bs_mnu_documents.mnu_more.new_book", @new_book_status, @bs_mnu_documents_more_id, '/goto?owner=dte&url=/book', @bs_mnu_documents_id, 0, @accion_modulo_pos_libro_mensual_new_id, 4),
                    ("bs_mnu_documents.mnu_more.calendar", @calendar_status, @bs_mnu_documents_more_id, '/goto?owner=bsale&url=/documents/calendar', @bs_mnu_documents_id, 0, @accion_modulo_pos_genera_documentos_new_id, 5),
                    ("bs_mnu_documents.mnu_more.withdrawal", @withdrawal_status, @bs_mnu_documents_more_id, '/goto?owner=bsale&url=/documents/deposit/withdrawal', @bs_mnu_documents_id, 0, @accion_modulo_pos_retiro_abono_cliente_new_id, 6),
                    ("bs_mnu_documents.mnu_more.dte_cession", @dte_cession_status, @bs_mnu_documents_more_id, '/goto?owner=dte&url=/cession', @bs_mnu_documents_id, 0, @accion_modulo_pos_cession_dte_new_id, 7),
                    ("bs_mnu_documents.mnu_more.milestone", @milestone_status, @bs_mnu_documents_more_id, '/goto?owner=bsale&url=/documents/milestone', @bs_mnu_documents_id, 0, @accion_modulo_pos_genera_documentos_new_id, 8),
                    ("bs_mnu_documents.mnu_more.import_book", @import_book_status, @bs_mnu_documents_more_id, @import_book_url, @bs_mnu_documents_id, 0, @accion_modulo_pos_libro_mensual_new_id, 9);

                -- CONSULTA DE CPE(PE) - CDFI(MX) 
                INSERT INTO `bs_temporary` 
                    (`ml_name`, `ml_active`, `ml_asociate`, `ml_url`, `m_id`, `ml_is_dropdown`, `id_accion_modulo`, `ml_order`)
                SELECT 
                    "bs_mnu_documents.mnu_more.summary_documents", @summary_documents_status, @bs_mnu_documents_more_id, '/goto?owner=summary_documents&url=/summary_documents', @bs_mnu_documents_id, 0, @accion_modulo_pos_genera_documentos_new_id, 10
                FROM DUAL
                WHERE EXISTS (
                    SELECT 1
                    FROM bs_menu_link
                    WHERE ml_name = 'bs_mnu_documents.mnu_more.summary_documents'
                    LIMIT 1
                );

                -- ABONOS DE CLIENTE
                INSERT INTO `bs_temporary` 
                    (`ml_name`, `ml_active`, `ml_asociate`, `ml_url`, `m_id`, `ml_is_dropdown`, `id_accion_modulo`, `ml_order`)
                SELECT 
                    "bs_mnu_documents.mnu_more.client_income", @client_income_status, @bs_mnu_documents_more_id, '/goto?owner=bsale&url=/documents/deposit/income', @bs_mnu_documents_id, 0, @accion_modulo_pos_abono_cliente_new_id, 11
                FROM DUAL
                WHERE EXISTS (
                    SELECT 1
                    FROM bs_menu_link
                    WHERE ml_name = 'bs_mnu_documents.mnu_more.client_income'
                    LIMIT 1
                );

                -- RECURRENCIA
                INSERT INTO `bs_temporary` 
                    (`ml_name`, `ml_active`, `ml_asociate`, `ml_url`, `m_id`, `ml_is_dropdown`, `id_accion_modulo`, `ml_order`)
                SELECT 
                    "bs_mnu_documents.mnu_more.recurrency", 1, @bs_mnu_documents_more_id, '/goto?owner=recurrency&url=/documents/recurrency', @bs_mnu_documents_id, 0, 0, 12
                FROM DUAL
                WHERE EXISTS (
                    SELECT 1
                    FROM bs_menu_link
                    WHERE ml_name = 'bs_mnu_documents.mnu_more.recurrency'
                    LIMIT 1
                );
                """
            r = self._execute_query(query, (), False, True)
            if not r["success"]:
                raise ValueError(r["error"])

            resp["success"] = True
            resp["row_affected"] = r["row_affected"]
        except ValueError as error:
            resp["error"] = str(error)
            resp["success"] = False
        return resp
 
 
    # SETTINGS   
    def set_menu_settings(self):
        resp = {}
        try:
            query = """
                SET @bs_mnu_settings_id = (
                    SELECT m_id
                    FROM bs_menu
                    WHERE m_name = 'bs_mnu_settings_v2'
                );

                INSERT INTO `bs_menu_link` 
                    (`ml_name`, `ml_active`, `ml_asociate`, `ml_url`, `m_id`, `ml_is_dropdown`, `id_accion_modulo`, `ml_order`)
                VALUES 
                    ("bs_mnu_settings.payment_methods", 1, 0, NULL, @bs_mnu_settings_id, 1, 0, 1);
                        
                SET @bs_mnu_payment_methods_id = (
                    SELECT ml_id
                    FROM bs_menu_link
                    WHERE ml_name = 'bs_mnu_settings.payment_methods'
                    AND m_id = @bs_mnu_settings_id
                );

                INSERT INTO `bs_menu_link` 
                    (`ml_name`, `ml_active`, `ml_asociate`, `ml_url`, `m_id`, `ml_is_dropdown`, `id_accion_modulo`, `ml_order`)
                VALUES 
                    ("bs_mnu_settings.integrations", 1, 0, NULL, @bs_mnu_settings_id, 1, 0, 4);
                        
                SET @bs_mnu_integrations_id = (
                    SELECT ml_id
                    FROM bs_menu_link
                    WHERE ml_name = 'bs_mnu_settings.integrations'
                    AND m_id = @bs_mnu_settings_id
                );

                INSERT INTO `bs_menu_link` 
                    (`ml_name`, `ml_active`, `ml_asociate`, `ml_url`, `m_id`, `ml_is_dropdown`, `id_accion_modulo`, `ml_order`)
                VALUES 
                    ("bs_mnu_settings.my_sales", 1, 0, NULL, @bs_mnu_settings_id, 1, 0, 2);
                        
                SET @bs_mnu_my_sales_id = (
                    SELECT ml_id
                    FROM bs_menu_link
                    WHERE ml_name = 'bs_mnu_settings.my_sales'
                    AND m_id = @bs_mnu_settings_id
                );

                INSERT INTO `bs_menu_link` 
                    (`ml_name`, `ml_active`, `ml_asociate`, `ml_url`, `m_id`, `ml_is_dropdown`, `id_accion_modulo`, `ml_order`)
                VALUES 
                    ("bs_mnu_settings.my_company", 1, 0, NULL, @bs_mnu_settings_id, 1, 0, 3);
                        
                SET @bs_mnu_my_company_id = (
                    SELECT ml_id
                    FROM bs_menu_link
                    WHERE ml_name = 'bs_mnu_settings.my_company'
                    AND m_id = @bs_mnu_settings_id
                );

                SET @accion_modulo_maes_sii_caf_new_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.maes.sii_caf.new' LIMIT 1);
                SET @accion_modulo_maes_condicion_venta_new_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.maes.condicion_venta.new' LIMIT 1);
                SET @accion_modulo_maes_condicion_venta_edit_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.maes.condicion_venta.edit' LIMIT 1);
                SET @accion_modulo_maes_impuestos_new_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.maes.impuestos.new' LIMIT 1);
                SET @accion_modulo_maes_impuestos_edit_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.maes.impuestos.edit' LIMIT 1);
                SET @accion_modulo_maes_monedas_new_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.maes.monedas.new' LIMIT 1);
                SET @accion_modulo_maes_monedas_edit_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.maes.monedas.edit' LIMIT 1);
                SET @accion_modulo_maes_renovacions_new_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.maes.renovacions.new' LIMIT 1);
                SET @accion_modulo_maes_renovacions_edit_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.maes.renovacions.edit' LIMIT 1);
                SET @accion_modulo_maes_forma_pagos_new_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.maes.forma_pagos.new' LIMIT 1);
                SET @accion_modulo_maes_forma_pagos_edit_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.maes.forma_pagos.edit' LIMIT 1);
                SET @accion_modulo_maes_tipo_documento_tributarios_new_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.maes.tipo_documento_tributarios.new' LIMIT 1);
                SET @accion_modulo_maes_tipo_documento_tributarios_edit_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.maes.tipo_documento_tributarios.edit' LIMIT 1);
                SET @accion_modulo_maes_bpoints_new_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.maes.bpoints.new' LIMIT 1);
                SET @accion_modulo_maes_bpoints_edit_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.maes.bpoints.edit' LIMIT 1);
                SET @accion_modulo_maes_sys_config_edit_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.maes.sys_config.edit' LIMIT 1);
                SET @accion_modulo_maes_sucursals_new_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.maes.sucursals.new' LIMIT 1);
                SET @accion_modulo_maes_sucursals_edit_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.maes.sucursals.edit' LIMIT 1);
                SET @accion_modulo_maes_usuarios_new_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.maes.usuarios.new' LIMIT 1);
                SET @accion_modulo_maes_usuarios_edit_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.maes.usuarios.edit' LIMIT 1);
                SET @accion_modulo_maes_perfiles_new_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.maes.perfiles.new' LIMIT 1);
                SET @accion_modulo_maes_perfiles_edit_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.maes.perfiles.edit' LIMIT 1);
                SET @online_store_status = (SELECT ml_active FROM bs_menu_link WHERE ml_name = 'bs_mnu_main.online_store' LIMIT 1);

                INSERT INTO `bs_temporary` 
                    (`ml_name`, `ml_active`, `ml_asociate`, `ml_url`, `m_id`, `ml_is_dropdown`, `id_accion_modulo`, `ml_order`)
                VALUES
                    ("bs_mnu_settings.config_pos", 1, @bs_mnu_payment_methods_id, '/goto?owner=bsale&url=/goto/config_pos', @bs_mnu_settings_id, 0, 0, 1),
                    ("bs_mnu_settings.payment_online_store", @online_store_status, @bs_mnu_payment_methods_id, '/goto?owner=market_admin&url=/admin/config/payments', @bs_mnu_settings_id, 0, @accion_modulo_pos_online_store_new_id, 2),
                    ("bs_mnu_settings.plugins", 1, @bs_mnu_integrations_id, '/goto?owner=extensions&url=/admin/config/bsale_store', @bs_mnu_settings_id, 0, 0, 1),
                    ("bs_mnu_settings.sii_caf", 1, @bs_mnu_my_sales_id, '/goto?owner=bsale&url=/admin/config/sii_caf', @bs_mnu_settings_id, 0, @accion_modulo_maes_sii_caf_new_id, 1),
                    ("bs_mnu_settings.sales_conditions", 1, @bs_mnu_my_sales_id, '/goto?owner=bsale&url=/admin/sales_conditions', @bs_mnu_settings_id, 0, @accion_modulo_maes_condicion_venta_new_id, 2),
                    ("bs_mnu_settings.sales_conditions", 1, @bs_mnu_my_sales_id, '/goto?owner=bsale&url=/admin/sales_conditions', @bs_mnu_settings_id, 0, @accion_modulo_maes_condicion_venta_edit_id, 2),
                    ("bs_mnu_settings.tax", 1, @bs_mnu_my_sales_id, '/goto?owner=bsale&url=/admin/impuestos', @bs_mnu_settings_id, 0, @accion_modulo_maes_impuestos_new_id, 3),
                    ("bs_mnu_settings.tax", 1, @bs_mnu_my_sales_id, '/goto?owner=bsale&url=/admin/impuestos', @bs_mnu_settings_id, 0, @accion_modulo_maes_impuestos_edit_id, 3),
                    ("bs_mnu_settings.currency", 1, @bs_mnu_my_sales_id, '/goto?owner=bsale&url=/admin/monedas', @bs_mnu_settings_id, 0, @accion_modulo_maes_monedas_new_id, 4),
                    ("bs_mnu_settings.currency", 1, @bs_mnu_my_sales_id, '/goto?owner=bsale&url=/admin/monedas', @bs_mnu_settings_id, 0, @accion_modulo_maes_monedas_edit_id, 4),
                    ("bs_mnu_settings.renovation", 1, @bs_mnu_my_sales_id, '/goto?owner=bsale&url=/admin/renovations', @bs_mnu_settings_id, 0, @accion_modulo_maes_renovacions_new_id, 5),
                    ("bs_mnu_settings.renovation", 1, @bs_mnu_my_sales_id, '/goto?owner=bsale&url=/admin/renovations', @bs_mnu_settings_id, 0, @accion_modulo_maes_renovacions_edit_id, 5),
                    ("bs_mnu_settings.payment_type", 1, @bs_mnu_my_sales_id, '/goto?owner=bsale&url=/admin/forma_pagos', @bs_mnu_settings_id, 0, @accion_modulo_maes_forma_pagos_new_id, 6),
                    ("bs_mnu_settings.payment_type", 1, @bs_mnu_my_sales_id, '/goto?owner=bsale&url=/admin/forma_pagos', @bs_mnu_settings_id, 0, @accion_modulo_maes_forma_pagos_edit_id, 6),
                    ("bs_mnu_settings.doc_type", 1, @bs_mnu_my_sales_id, '/goto?owner=bsale&url=/admin/tipo_documento_tributarios', @bs_mnu_settings_id, 0, @accion_modulo_maes_tipo_documento_tributarios_new_id, 7),
                    ("bs_mnu_settings.doc_type", 1, @bs_mnu_my_sales_id, '/goto?owner=bsale&url=/admin/tipo_documento_tributarios', @bs_mnu_settings_id, 0, @accion_modulo_maes_tipo_documento_tributarios_edit_id, 7),
                    ("bs_mnu_settings.points_program", 1, @bs_mnu_my_sales_id, '/goto?owner=bsale&url=/admin/bpoints/settings', @bs_mnu_settings_id, 0, @accion_modulo_maes_bpoints_new_id, 8),
                    ("bs_mnu_settings.points_program", 1, @bs_mnu_my_sales_id, '/goto?owner=bsale&url=/admin/bpoints/settings', @bs_mnu_settings_id, 0, @accion_modulo_maes_bpoints_edit_id, 8),
                    ("bs_mnu_settings.sys_config", 1, @bs_mnu_my_company_id, '/goto?owner=bsale&url=/admin/config/sys_config', @bs_mnu_settings_id, 0, @accion_modulo_maes_sys_config_edit_id, 1),
                    ("bs_mnu_settings.office", 1, @bs_mnu_my_company_id, '/goto?owner=bsale&url=/admin/sucursales', @bs_mnu_settings_id, 0, @accion_modulo_maes_sucursals_new_id, 2),
                    ("bs_mnu_settings.office", 1, @bs_mnu_my_company_id, '/goto?owner=bsale&url=/admin/sucursales', @bs_mnu_settings_id, 0, @accion_modulo_maes_sucursals_edit_id, 2),
                    ("bs_mnu_settings.users", 1, @bs_mnu_my_company_id, '/goto?owner=bsale&url=/admin/usuarios', @bs_mnu_settings_id, 0, @accion_modulo_maes_usuarios_new_id, 3),
                    ("bs_mnu_settings.users", 1, @bs_mnu_my_company_id, '/goto?owner=bsale&url=/admin/usuarios', @bs_mnu_settings_id, 0, @accion_modulo_maes_usuarios_edit_id, 3),
                    ("bs_mnu_settings.access_profile", 1, @bs_mnu_my_company_id, '/goto?owner=bsale&url=/admin/perfiles', @bs_mnu_settings_id, 0, @accion_modulo_maes_perfiles_new_id, 4),
                    ("bs_mnu_settings.access_profile", 1, @bs_mnu_my_company_id, '/goto?owner=bsale&url=/admin/perfiles', @bs_mnu_settings_id, 0, @accion_modulo_maes_perfiles_edit_id, 4),
                    ("bs_mnu_settings.payment_portal", 1, @bs_mnu_my_company_id, '/goto?owner=bsale&url=/goto/my_invoice?renderAddServices=1', @bs_mnu_settings_id, 0, @accion_modulo_maes_forma_pagos_new_id, 5),
                    ("bs_mnu_settings.payment_portal", 1, @bs_mnu_my_company_id, '/goto?owner=bsale&url=/goto/my_invoice?renderAddServices=1', @bs_mnu_settings_id, 0, @accion_modulo_maes_forma_pagos_edit_id, 5);

                -- RETENCIONES MX
                INSERT INTO `bs_temporary` 
                    (`ml_name`, `ml_active`, `ml_asociate`, `ml_url`, `m_id`, `ml_is_dropdown`, `id_accion_modulo`, `ml_order`)
                SELECT
                    "bs_mnu_settings.retentions", 1, @bs_mnu_my_sales_id, '/goto?owner=bsale&url=/admin/retenciones', @bs_mnu_settings_id, 0, @accion_modulo_maes_impuestos_new_id, 9
                FROM DUAL
                WHERE EXISTS (
                    SELECT 1 FROM sys_config WHERE variable = 'country' AND valor = 'mx' LIMIT 1
                )
                UNION ALL
                SELECT
                    "bs_mnu_settings.retentions", 1, @bs_mnu_my_sales_id, '/goto?owner=bsale&url=/admin/retenciones', @bs_mnu_settings_id, 0, @accion_modulo_maes_impuestos_edit_id, 9
                FROM DUAL
                WHERE EXISTS (
                    SELECT 1 FROM sys_config WHERE variable = 'country' AND valor = 'mx' LIMIT 1
                );
            """
            r = self._execute_query(query, (), False, True)
            if not r["success"]:
                raise ValueError(r["error"])

            resp["success"] = True
            resp["row_affected"] = r["row_affected"]
        except ValueError as error:
            resp["error"] = str(error)
            resp["success"] = False
        return resp


    # REPORTES       
    def set_menu_reports(self):
        resp = {}
        try:
            query = """
                -- MENU "VENTAS"
                SET @bs_mnu_reports_id = (
                    SELECT m_id
                    FROM bs_menu
                    WHERE m_name = 'bs_mnu_reports_v2'
                );

                SET @dashboard_status = (SELECT COALESCE(MAX(valor), 0) FROM sys_config WHERE variable = 'reports_v2_dashboard_resumen');
                SET @dashboard_url = (SELECT COALESCE(MAX(ml_url), '/goto?owner=reports_v2&url=/sales/dashboard') FROM bs_menu_link WHERE ml_name = 'reports_v2.dashboard' LIMIT 1);

                INSERT INTO `bs_menu_link`
                    (`ml_name`, `ml_active`, `ml_asociate`, `ml_url`, `m_id`, `ml_is_dropdown`, `id_accion_modulo`, `ml_order`)
                VALUES
                    ("reports_v2.dashboard", @dashboard_status, 0, @dashboard_url, @bs_mnu_reports_id, 0, 0, 1);

                INSERT INTO `bs_menu_link` 
                    (`ml_name`, `ml_active`, `ml_asociate`, `ml_url`, `m_id`, `ml_is_dropdown`, `id_accion_modulo`, `ml_order`)
                VALUES 
                    ("reports.menu.sales", 1, 0, NULL, @bs_mnu_reports_id, 1, 0, 2);
                    
                SET @report_menu_sales_id = (
                    SELECT ml_id
                    FROM bs_menu_link
                    WHERE ml_name = 'reports.menu.sales'
                    AND m_id = @bs_mnu_reports_id
                );

                SET @accion_modulo_sales_overview_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.reports_v2.sales_overview.view' LIMIT 1);
                SET @sales_overview_status = (SELECT ml_active FROM bs_menu_link WHERE ml_name = 'reports_v2.sales_overview' LIMIT 1);
                SET @sales_overview_url = (SELECT ml_url FROM bs_menu_link WHERE ml_name = 'reports_v2.sales_overview' LIMIT 1);
                SET @accion_modulo_sales_reports_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.reports_v2.sales_reports.view' LIMIT 1);
                SET @sales_details_status = (SELECT ml_active FROM bs_menu_link WHERE ml_name = 'reports_v2.sales_details' LIMIT 1);
                SET @sales_details_url = (SELECT ml_url FROM bs_menu_link WHERE ml_name = 'reports_v2.sales_details' LIMIT 1);
                SET @payment_methods_status = (SELECT ml_active FROM bs_menu_link WHERE ml_name = 'reports_v2.payment_methods' LIMIT 1);
                SET @payment_methods_url = (SELECT ml_url FROM bs_menu_link WHERE ml_name = 'reports_v2.payment_methods' LIMIT 1);
                SET @accion_modulo_reports_return_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.reports.returns.view' LIMIT 1);
                SET @accion_modulo_reports_my_sales_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.reports.my_sales.view' LIMIT 1);
                SET @my_sales_url = (SELECT ml_url FROM bs_menu_link WHERE ml_name = 'reports.my_sales' LIMIT 1);

                INSERT INTO `bs_temporary` 
                    (`ml_name`, `ml_active`, `ml_asociate`, `ml_url`, `m_id`, `ml_is_dropdown`, `id_accion_modulo`, `ml_order`)
                VALUES
                    ("reports_v2.sales_overview", @sales_overview_status, @report_menu_sales_id , @sales_overview_url, @bs_mnu_reports_id, 0, @accion_modulo_sales_overview_id, 1),
                    ("reports_v2.sales_details", @sales_details_status, @report_menu_sales_id , @sales_details_url, @bs_mnu_reports_id, 0, @accion_modulo_sales_reports_id, 2),
                    ("reports_v2.payment_methods", @payment_methods_status, @report_menu_sales_id , @payment_methods_url, @bs_mnu_reports_id, 0, @accion_modulo_sales_reports_id, 4),
                    ("reports.returns", 1, @report_menu_sales_id , '/goto?owner=bsale&url=/reports/returns', @bs_mnu_reports_id, 0, @accion_modulo_reports_return_id, 5),
                    ("reports.my_sales", 1, @report_menu_sales_id , @my_sales_url, @bs_mnu_reports_id, 0, @accion_modulo_reports_my_sales_id, 6);
                COMMIT;

                -- MENU "ANALISIS"
                INSERT INTO `bs_menu_link` 
                    (`ml_name`, `ml_active`, `ml_asociate`, `ml_url`, `m_id`, `ml_is_dropdown`, `id_accion_modulo`, `ml_order`)
                VALUES 
                    ("reports.menu.analysis", 1, 0, NULL, @bs_mnu_reports_id, 1, 0, 3);
                    
                SET @report_menu_analysis_id = (
                    SELECT ml_id
                    FROM bs_menu_link
                    WHERE ml_name = 'reports.menu.analysis'
                    AND m_id = @bs_mnu_reports_id
                );

                SET @accion_modulo_reports_dynamic_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.reports.dynamic.view' LIMIT 1);

                INSERT INTO `bs_temporary` 
                    (`ml_name`, `ml_active`, `ml_asociate`, `ml_url`, `m_id`, `ml_is_dropdown`, `id_accion_modulo`, `ml_order`)
                VALUES
                    ("reports.dynamic_sales", 1, @report_menu_analysis_id , '/goto?owner=bsale&url=/reports/dynamic', @bs_mnu_reports_id, 0, @accion_modulo_reports_dynamic_id, 1),
                    ("reports.compare", 1, @report_menu_analysis_id , '/goto?owner=bsale&url=/reports/sales_by_product&compare=true', @bs_mnu_reports_id, 0, @accion_modulo_sales_reports_id, 2);


                -- MENU "OTROS"
                INSERT INTO `bs_menu_link` 
                    (`ml_name`, `ml_active`, `ml_asociate`, `ml_url`, `m_id`, `ml_is_dropdown`, `id_accion_modulo`, `ml_order`)
                VALUES 
                    ("reports.menu.others", 1, 0, NULL, @bs_mnu_reports_id, 1, 0, 4);
                    
                SET @report_menu_others_id = (
                    SELECT ml_id
                    FROM bs_menu_link
                    WHERE ml_name = 'reports.menu.others'
                    AND m_id = @bs_mnu_reports_id
                );

                SET @accion_modulo_reports_discount_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.reports.discount.view' LIMIT 1);
                SET @accion_modulo_reports_coupons_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.reports.coupons.view' LIMIT 1);
                SET @accion_modulo_reports_giftcards_view_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.reports.giftcards.view' LIMIT 1);
                SET @report_market_status = (SELECT ml_active FROM bs_menu_link WHERE ml_name = 'reports.market' LIMIT 1);
                SET @giftcards_status = (SELECT ml_active FROM bs_menu_link WHERE ml_name = 'reports.giftcards' LIMIT 1);
                SET @accion_modulo_maes_bpoints_new_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.maes.bpoints.new' LIMIT 1);
                SET @accion_modulo_maes_bpoints_edit_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.maes.bpoints.edit' LIMIT 1);
                SET @points_status = (SELECT ml_active FROM bs_menu_link WHERE ml_name = 'reports.bpoints' LIMIT 1);
                SET @accion_modulo_pos_online_orders_new_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.pos.online_orders.new' LIMIT 1);

                INSERT INTO `bs_temporary` 
                    (`ml_name`, `ml_active`, `ml_asociate`, `ml_url`, `m_id`, `ml_is_dropdown`, `id_accion_modulo`, `ml_order`)
                VALUES
                    ("reports.discount", 1, @report_menu_others_id , '/goto?owner=bsale&url=/reports/discount', @bs_mnu_reports_id, 0, @accion_modulo_reports_discount_id, 1),
                    ("reports.coupons", 1, @report_menu_others_id , '/goto?owner=discount&url=/admin/coupons/report', @bs_mnu_reports_id, 0, @accion_modulo_reports_coupons_id, 2),
                    ("reports.lost_cart", @report_market_status, @report_menu_others_id , '/goto?owner=lost_cart&url=/admin/lost_cart/metrics', @bs_mnu_reports_id, 0, @accion_modulo_pos_online_orders_new_id, 3),
                    ("reports.giftcards", @giftcards_status, @report_menu_others_id , '/goto?owner=loyalty_v2&url=/admin/giftcard/movements', @bs_mnu_reports_id, 0, @accion_modulo_reports_giftcards_view_id, 4),
                    ("reports.bpoints", @points_status, @report_menu_others_id , '/goto?owner=bsale&url=/reports/bpoints', @bs_mnu_reports_id, 0, @accion_modulo_maes_bpoints_new_id, 5),
                    ("reports.bpoints", @points_status, @report_menu_others_id , '/goto?owner=bsale&url=/reports/bpoints', @bs_mnu_reports_id, 0, @accion_modulo_maes_bpoints_edit_id, 5);

                -- MENU "STOCK"
                SET @mnu_report_stock_status = (SELECT ml_active FROM bs_menu_link WHERE ml_name = 'reports.menu.stock' LIMIT 1);
                INSERT INTO `bs_menu_link` 
                    (`ml_name`, `ml_active`, `ml_asociate`, `ml_url`, `m_id`, `ml_is_dropdown`, `id_accion_modulo`, `ml_order`)
                VALUES 
                    ("reports.menu.stock", @mnu_report_stock_status, 0, NULL, @bs_mnu_reports_id, 1, 0, 5);
                    
                SET @report_menu_stock_id = (
                    SELECT ml_id
                    FROM bs_menu_link
                    WHERE ml_name = 'reports.menu.stock'
                    AND m_id = @bs_mnu_reports_id
                );

                SET @accion_modulo_reports_stock_view_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.reports.stock.view' LIMIT 1);
                SET @accion_modulo_reports_shipping_view_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.reports.shipping.view' LIMIT 1);
                SET @accion_modulo_reports_dynamic_view_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.reports.dynamic.view' LIMIT 1);

                INSERT INTO `bs_temporary` 
                    (`ml_name`, `ml_active`, `ml_asociate`, `ml_url`, `m_id`, `ml_is_dropdown`, `id_accion_modulo`, `ml_order`)
                VALUES
                    ("reports.stock_actual", 1, @report_menu_stock_id, @stock_actual_url, @bs_mnu_reports_id, 0, @accion_modulo_reports_stock_view_id, 1),
                    ("reports.stock_card", 1, @report_menu_stock_id, '/goto?owner=bsale&url=/reports/stock/card', @bs_mnu_reports_id, 0, @accion_modulo_reports_stock_view_id, 2),
                    ("reports.reposicion", 1, @report_menu_stock_id, '/goto?owner=bsale&url=/reports/replenishment', @bs_mnu_reports_id, 0, @accion_modulo_reports_stock_view_id, 3),
                    ("reports.reserved_stock", 1, @report_menu_stock_id, '/goto?owner=bsale&url=/reports/stock/reserved', @bs_mnu_reports_id, 0, @accion_modulo_reports_stock_view_id, 4),
                    ("reports.fifo_availability", 1, @report_menu_stock_id, '/goto?owner=bsale&url=/reports/stock/fifo_availability', @bs_mnu_reports_id, 0, @accion_modulo_reports_stock_view_id, 5),
                    ("reports.stock_to_date", 1, @report_menu_stock_id, '/goto?owner=bsale&url=/reports/stock/to_date', @bs_mnu_reports_id, 0, @accion_modulo_reports_stock_view_id, 6),
                    ("reports.inventory", 1, @report_menu_stock_id, '/goto?owner=bsale&url=/reports/stock/inventory', @bs_mnu_reports_id, 0, @accion_modulo_reports_stock_view_id, 7),
                    ("reports.stock_recepcion", 1, @report_menu_stock_id, '/goto?owner=bsale&url=/reports/stock/reception', @bs_mnu_reports_id, 0, @accion_modulo_reports_stock_view_id, 8),
                    ("reports.stock_consumption", 1, @report_menu_stock_id, '/goto?owner=bsale&url=/reports/stock/consumption', @bs_mnu_reports_id, 0, @accion_modulo_reports_stock_view_id, 9),
                    ("reports.shipping", 1, @report_menu_stock_id, '/goto?owner=bsale&url=/reports/shipping', @bs_mnu_reports_id, 0, @accion_modulo_reports_shipping_view_id, 10),
                    ("reports.shipping_transit", 1, @report_menu_stock_id, '/goto?owner=bsale&url=/reports/shipping/transit', @bs_mnu_reports_id, 0, @accion_modulo_reports_shipping_view_id, 11),
                    ("reports.serials", 1, @report_menu_stock_id, '/goto?owner=bsale&url=/reports/stock/serials', @bs_mnu_reports_id, 0, @accion_modulo_reports_stock_view_id, 12),
                    ("reports.dynamic_stock", @sys_stock_plan_active, @report_menu_stock_id, '/goto?owner=bsale&url=/reports/stock/dynamic', @bs_mnu_reports_id, 0, @accion_modulo_reports_dynamic_view_id, 13),
                    ("reports.stock_updates", 1, @report_menu_stock_id, '/goto?owner=bsale&url=/reports/stock/updates', @bs_mnu_reports_id, 0, @accion_modulo_reports_stock_view_id, 14),
                    ("reports.stock_rotation", 1, @report_menu_stock_id, '/goto?owner=bsale&url=/reports/stock/rotation', @bs_mnu_reports_id, 0, @accion_modulo_reports_stock_view_id, 15);

                -- KARDEX
                INSERT INTO `bs_temporary` 
                    (`ml_name`, `ml_active`, `ml_asociate`, `ml_url`, `m_id`, `ml_is_dropdown`, `id_accion_modulo`, `ml_order`)
                SELECT 
                    "reports.kardex", 1, @report_menu_stock_id, '/goto?owner=bsale_report_pe&url=/pe/reports/stock/kardex', @bs_mnu_reports_id, 0, @accion_modulo_reports_stock_view_id, 16
                FROM DUAL
                WHERE EXISTS (
                    SELECT 1
                    FROM bs_menu_link
                    WHERE ml_name = 'reports.kardex'
                    LIMIT 1
                );

                -- MENU "DOCUMENTOS"
                INSERT INTO `bs_menu_link` 
                    (`ml_name`, `ml_active`, `ml_asociate`, `ml_url`, `m_id`, `ml_is_dropdown`, `id_accion_modulo`, `ml_order`)
                VALUES 
                    ("reports.menu.documents", 1, 0, NULL, @bs_mnu_reports_id, 1, 0, 6);
                    
                SET @report_menu_documents_id = (
                    SELECT ml_id
                    FROM bs_menu_link
                    WHERE ml_name = 'reports.menu.documents'
                    AND m_id = @bs_mnu_reports_id
                );

                SET @accion_modulo_reports_sent_documents_view_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.reports.sent_documents.view' LIMIT 1);
                SET @accion_modulo_reports_pending_docs_view_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.reports.pending_docs.view' LIMIT 1);
                SET @accion_modulo_reports_reprint_view_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.reports.reprint.view' LIMIT 1);
                SET @accion_modulo_reports_third_party_docs_view_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.reports.third_party_docs.view' LIMIT 1);
                SET @accion_modulo_reports_libro_mensual_view_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.reports.libro_mensual.view' LIMIT 1);
                SET @libro_mensual_status = (SELECT ml_active FROM bs_menu_link WHERE ml_name = 'reports.libro_mensual' LIMIT 1);
                SET @accion_modulo_reports_documents_by_currency_view_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.reports.documents_by_currency.view' LIMIT 1);
                SET @reports_claimed_status = (SELECT ml_active FROM bs_menu_link WHERE ml_name = 'reports.claimed' LIMIT 1);

                INSERT INTO `bs_temporary` 
                    (`ml_name`, `ml_active`, `ml_asociate`, `ml_url`, `m_id`, `ml_is_dropdown`, `id_accion_modulo`, `ml_order`)
                VALUES
                    ("reports.sent_documents", 1, @report_menu_documents_id, '/goto?owner=bsale&url=/reports/sent_documents', @bs_mnu_reports_id, 0, @accion_modulo_reports_sent_documents_view_id, 1),
                    ("reports.pending_docs", 1, @report_menu_documents_id, '/goto?owner=bsale&url=/reports/pending_docs', @bs_mnu_reports_id, 0, @accion_modulo_reports_pending_docs_view_id, 2),
                    ("reports.reprint", 1, @report_menu_documents_id, '/goto?owner=bsale&url=/reports/reprint', @bs_mnu_reports_id, 0, @accion_modulo_reports_reprint_view_id, 3),
                    ("reports.third_party_docs", 1, @report_menu_documents_id, '/goto?owner=bsale&url=/reports/third_party_docs', @bs_mnu_reports_id, 0, @accion_modulo_reports_third_party_docs_view_id, 4),
                    ("reports.libro_mensual", @libro_mensual_status, @report_menu_documents_id, '/goto?owner=bsale&url=/reports/libro_mensual', @bs_mnu_reports_id, 0, @accion_modulo_reports_libro_mensual_view_id, 5),
                    ("reports.documents_by_currency", 1, @report_menu_documents_id, '/goto?owner=bsale&url=/reports/documents_by_currency', @bs_mnu_reports_id, 0, @accion_modulo_reports_documents_by_currency_view_id, 7);

                -- DOCUMENTOS RECLAMADOS
                INSERT INTO `bs_temporary` 
                    (`ml_name`, `ml_active`, `ml_asociate`, `ml_url`, `m_id`, `ml_is_dropdown`, `id_accion_modulo`, `ml_order`)
                SELECT 
                    "reports.claimed", @reports_claimed_status, @report_menu_documents_id, '/goto?owner=dte_v2&url=/../dte_claimed/?', @bs_mnu_reports_id, 0, 0, 6
                FROM DUAL
                WHERE EXISTS (
                    SELECT 1
                    FROM bs_menu_link
                    WHERE ml_name = 'reports.claimed'
                    LIMIT 1
                );

                -- MENU "PAGOS"
                INSERT INTO `bs_menu_link` 
                    (`ml_name`, `ml_active`, `ml_asociate`, `ml_url`, `m_id`, `ml_is_dropdown`, `id_accion_modulo`, `ml_order`)
                VALUES 
                    ("reports.menu.payments", 1, 0, NULL, @bs_mnu_reports_id, 1, 0, 7);
                    
                SET @report_menu_payments_id = (
                    SELECT ml_id
                    FROM bs_menu_link
                    WHERE ml_name = 'reports.menu.payments'
                    AND m_id = @bs_mnu_reports_id
                );

                SET @accion_modulo_reports_pos_cash_flow_view_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.reports.pos_cash_flow.view' LIMIT 1);
                SET @accion_modulo_reports_client_debt_view_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.reports.client_debt.view' LIMIT 1);
                SET @accion_modulo_reports_deposits_view_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.reports.deposits.view' LIMIT 1);
                SET @accion_modulo_reports_checks_to_cash_view_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.reports.checks_to_cash.view' LIMIT 1);
                SET @accion_modulo_pos_online_orders_new_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.pos.online_orders.new' LIMIT 1);

                INSERT INTO `bs_temporary` 
                    (`ml_name`, `ml_active`, `ml_asociate`, `ml_url`, `m_id`, `ml_is_dropdown`, `id_accion_modulo`, `ml_order`)
                VALUES
                    ("reports.pos_cash_flow", 1, @report_menu_payments_id, '/goto?owner=bsale&url=/reports/pos_cash_flow', @bs_mnu_reports_id, 0, @accion_modulo_reports_pos_cash_flow_view_id, 1),
                    ("reports.client_debt_card", 1, @report_menu_payments_id, '/goto?owner=bsale&url=/reports/clients/card', @bs_mnu_reports_id, 0, @accion_modulo_reports_client_debt_view_id, 2),
                    ("reports.market_payment", @report_market_status, @report_menu_payments_id, '/goto?owner=market_payment_report&url=/reports', @bs_mnu_reports_id, 0, @accion_modulo_pos_online_orders_new_id, 3),
                    ("reports.deuda_cliente", 1, @report_menu_payments_id, '/goto?owner=bsale&url=/reports/client_debt', @bs_mnu_reports_id, 0, @accion_modulo_reports_client_debt_view_id, 4),
                    ("reports.pago_credito_documentos", 1, @report_menu_payments_id, '/goto?owner=bsale&url=/reports/client_debt/payment', @bs_mnu_reports_id, 0, @accion_modulo_reports_client_debt_view_id, 5),
                    ("reports.client_deposits", 1, @report_menu_payments_id, '/goto?owner=bsale&url=/reports/client_deposits', @bs_mnu_reports_id, 0, @accion_modulo_reports_deposits_view_id, 6),
                    ("reports.checks_to_cash", 1, @report_menu_payments_id, '/goto?owner=bsale&url=/reports/checks_to_cash', @bs_mnu_reports_id, 0, @accion_modulo_reports_checks_to_cash_view_id, 7);
            """
            r = self._execute_query(query, (), False, True)
            if not r["success"]:
                raise ValueError(r["error"])

            resp["success"] = True
            resp["row_affected"] = r["row_affected"]
        except ValueError as error:
            resp["error"] = str(error)
            resp["success"] = False
        return resp


    # TIENDA EN LINEA       
    def set_menu_online_store(self):
        resp = {}
        try:
            query = """
                -- MENU "TIENDA EN LINEA"
                SET @bs_mnu_online_store_id = (
                    SELECT m_id
                    FROM bs_menu
                    WHERE m_name = 'bs_mnu_online_store_v2'
                );

                -- ACCESOS RAPIDOS
                INSERT INTO `bs_menu_link` 
                    (`ml_name`, `ml_active`, `ml_asociate`, `ml_url`, `m_id`, `ml_is_dropdown`, `id_accion_modulo`, `ml_order`)
                VALUES 
                    ("bs_mnu_online_store.shortcuts", 1, 0, NULL, @bs_mnu_online_store_id, 1, 0, 1);
                    
                SET @bs_mnu_shortcuts_id = (
                    SELECT ml_id
                    FROM bs_menu_link
                    WHERE ml_name = 'bs_mnu_online_store.shortcuts'
                    AND m_id = @bs_mnu_online_store_id
                );

                -- COLLECCIONES
                INSERT INTO `bs_temporary` 
                    (`ml_name`, `ml_active`, `ml_asociate`, `ml_url`, `m_id`, `ml_is_dropdown`, `id_accion_modulo`, `ml_order`)
                VALUES 
                    ("bs_mnu_online_store.collection", 1, 0, "/goto?owner=market_admin&url=/admin/config/collections", @bs_mnu_online_store_id, 0, 0, 2);

                SET @accion_modulo_maes_products_new_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.maes.productos.new' LIMIT 1);
                SET @accion_modulo_maes_products_edit_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.maes.productos.edit' LIMIT 1);
                SET @accion_modulo_pos_online_orders_new_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.pos.online_orders.new' LIMIT 1);

                INSERT INTO `bs_temporary` 
                    (`ml_name`, `ml_active`, `ml_asociate`, `ml_url`, `m_id`, `ml_is_dropdown`, `id_accion_modulo`, `ml_order`)
                VALUES 
                    ("bs_mnu_online_store.orders", 1, 0, '/goto?owner=document&url=/orders/status/', @bs_mnu_online_store_id, 0, @accion_modulo_pos_online_orders_new_id, 3);

                SET @accion_modulo_maes_cupones_new_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.maes.cupones.new' LIMIT 1);
                SET @accion_modulo_maes_cupones_edit_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.maes.cupones.edit' LIMIT 1);
                SET @accion_modulo_maes_descuentos_new_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.maes.descuentos.new' LIMIT 1);
                SET @accion_modulo_maes_descuentos_edit_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.maes.descuentos.edit' LIMIT 1);

                INSERT INTO `bs_menu_link` 
                    (`ml_name`, `ml_active`, `ml_asociate`, `ml_url`, `m_id`, `ml_is_dropdown`, `id_accion_modulo`, `ml_order`)
                VALUES
                    ("bs_mnu_online_store.shipping", 1, 0, NULL, @bs_mnu_online_store_id, 1, 0, 4);

                SET @bs_mnu_shipping_id = (
                    SELECT ml_id
                    FROM bs_menu_link
                    WHERE ml_name = 'bs_mnu_online_store.shipping'
                    AND m_id = @bs_mnu_online_store_id
                );

                INSERT INTO `bs_menu_link` 
                    (`ml_name`, `ml_active`, `ml_asociate`, `ml_url`, `m_id`, `ml_is_dropdown`, `id_accion_modulo`, `ml_order`)
                VALUES
                    ("bs_mnu_online_store.design", 1, 0, NULL, @bs_mnu_online_store_id, 1, 0, 5);
                    
                SET @bs_mnu_design_id = (
                    SELECT ml_id
                    FROM bs_menu_link
                    WHERE ml_name = 'bs_mnu_online_store.design'
                    AND m_id = @bs_mnu_online_store_id
                );

                INSERT INTO `bs_menu_link` 
                    (`ml_name`, `ml_active`, `ml_asociate`, `ml_url`, `m_id`, `ml_is_dropdown`, `id_accion_modulo`, `ml_order`)
                VALUES
                    ("bs_mnu_online_store.marketing", 1, 0, NULL, @bs_mnu_online_store_id, 1, 0, 6);
                    
                SET @bs_mnu_marketing_id = (
                    SELECT ml_id
                    FROM bs_menu_link
                    WHERE ml_name = 'bs_mnu_online_store.marketing'
                    AND m_id = @bs_mnu_online_store_id
                );

                INSERT INTO `bs_menu_link` 
                    (`ml_name`, `ml_active`, `ml_asociate`, `ml_url`, `m_id`, `ml_is_dropdown`, `id_accion_modulo`, `ml_order`)
                VALUES
                    ("bs_mnu_online_store.config", 1, 0, NULL, @bs_mnu_online_store_id, 1, 0, 7);
                    
                SET @bs_mnu_config_id = (
                    SELECT ml_id
                    FROM bs_menu_link
                    WHERE ml_name = 'bs_mnu_online_store.config'
                    AND m_id = @bs_mnu_online_store_id
                );

                INSERT INTO `bs_temporary` 
                    (`ml_name`, `ml_active`, `ml_asociate`, `ml_url`, `m_id`, `ml_is_dropdown`, `id_accion_modulo`, `ml_order`)
                VALUES
                    ("bs_mnu_online_store.mnu_shortcuts.productweb", 1, @bs_mnu_shortcuts_id, '/goto?owner=product_admin&url=/admin/products', @bs_mnu_online_store_id, 0, @accion_modulo_maes_products_new_id, 1),
                    ("bs_mnu_online_store.mnu_shortcuts.productweb", 1, @bs_mnu_shortcuts_id, '/goto?owner=product_admin&url=/admin/products', @bs_mnu_online_store_id, 0, @accion_modulo_maes_products_edit_id, 1),
                    ("bs_mnu_online_store.mnu_shortcuts.collections", 1, @bs_mnu_shortcuts_id, '/goto?owner=market_admin&url=/admin/config/collections', @bs_mnu_online_store_id, 0, 0, 2),
                    ("bs_mnu_online_store.mnu_shortcuts.orders", 1, @bs_mnu_shortcuts_id, '/goto?owner=document&url=/orders/status/', @bs_mnu_online_store_id, 0, @accion_modulo_pos_online_orders_new_id, 3),
                    ("bs_mnu_online_store.mnu_shortcuts.marketing_events", 1, @bs_mnu_shortcuts_id, '/goto?owner=marketing_events&url=/admin/events', @bs_mnu_online_store_id, 0, 0, 4),
                    ("bs_mnu_online_store.mnu_shortcuts.coupons", 1, @bs_mnu_shortcuts_id, '/goto?owner=discount&url=/admin/coupons', @bs_mnu_online_store_id, 0, @accion_modulo_maes_cupones_new_id, 5),
                    ("bs_mnu_online_store.mnu_shortcuts.coupons", 1, @bs_mnu_shortcuts_id, '/goto?owner=discount&url=/admin/coupons', @bs_mnu_online_store_id, 0, @accion_modulo_maes_cupones_edit_id, 5),
                    ("bs_mnu_online_store.mnu_shortcuts.discount", 1, @bs_mnu_shortcuts_id, '/goto?owner=discount&url=/admin/discounts', @bs_mnu_online_store_id, 0, @accion_modulo_maes_descuentos_new_id, 6),
                    ("bs_mnu_online_store.mnu_shortcuts.discount", 1, @bs_mnu_shortcuts_id, '/goto?owner=discount&url=/admin/discounts', @bs_mnu_online_store_id, 0, @accion_modulo_maes_descuentos_edit_id, 6),
                    ("bs_mnu_online_store.mnu_shipping.import_shipping", 1, @bs_mnu_shipping_id, '/goto?owner=market&url=/admin/import/shipping', @bs_mnu_online_store_id, 0, 0, 1),
                    ("bs_mnu_online_store.mnu_shipping.shipping", 1, @bs_mnu_shipping_id, '/goto?owner=shipping&url=/admin/shipping/courier', @bs_mnu_online_store_id, 0, 0, 2),
                    ("bs_mnu_online_store.mnu_design.template", 1, @bs_mnu_design_id, '/goto?owner=market&url=/admin/template', @bs_mnu_online_store_id, 0, 0, 1),
                    ("bs_mnu_online_store.mnu_design.component", 1, @bs_mnu_design_id, '/goto?owner=market&url=/admin/component', @bs_mnu_online_store_id, 0, 0, 2),
                    ("bs_mnu_online_store.mnu_design.file", 1, @bs_mnu_design_id, '/goto?owner=market&url=/admin/file', @bs_mnu_online_store_id, 0, 0, 3),
                    ("bs_mnu_online_store.mnu_design.slider", 1, @bs_mnu_design_id, '/goto?owner=market_admin&url=/admin/config/sliders', @bs_mnu_online_store_id, 0, 0, 4),
                    ("bs_mnu_online_store.mnu_design.images", 1, @bs_mnu_design_id, '/goto?owner=market_admin&url=/admin/config/images', @bs_mnu_online_store_id, 0, 0, 5),
                    ("bs_mnu_online_store.mnu_design.forms", 1, @bs_mnu_design_id, '/goto?owner=market_admin&url=/admin/config/forms', @bs_mnu_online_store_id, 0, 0, 6),
                    ("bs_mnu_online_store.mnu_design.articles", 1, @bs_mnu_design_id, '/goto?owner=market_admin&url=/admin/config/articles', @bs_mnu_online_store_id, 0, 0, 7),
                    ("bs_mnu_online_store.mnu_design.navegation", 1, @bs_mnu_design_id, '/goto?owner=design&url=/admin/link', @bs_mnu_online_store_id, 0, 0, 8),
                    ("bs_mnu_online_store.mnu_marketing.campaign", 1, @bs_mnu_marketing_id, '/goto?owner=campaign&url=/admin/campaigns', @bs_mnu_online_store_id, 0, 0, 1),
                    ("bs_mnu_online_store.mnu_marketing.coupons", 1, @bs_mnu_marketing_id, '/goto?owner=discount&url=/admin/coupons', @bs_mnu_online_store_id, 0, @accion_modulo_maes_cupones_new_id, 2),
                    ("bs_mnu_online_store.mnu_marketing.coupons", 1, @bs_mnu_marketing_id, '/goto?owner=discount&url=/admin/coupons', @bs_mnu_online_store_id, 0, @accion_modulo_maes_cupones_edit_id, 2),
                    ("bs_mnu_online_store.mnu_marketing.discount", 1, @bs_mnu_marketing_id, '/goto?owner=discount&url=/admin/discounts', @bs_mnu_online_store_id, 0, @accion_modulo_maes_descuentos_new_id, 3),
                    ("bs_mnu_online_store.mnu_marketing.discount", 1, @bs_mnu_marketing_id, '/goto?owner=discount&url=/admin/discounts', @bs_mnu_online_store_id, 0, @accion_modulo_maes_descuentos_edit_id, 3),
                    ("bs_mnu_online_store.mnu_marketing.lost_cart", 1, @bs_mnu_marketing_id, '/goto?owner=lost_cart&url=/admin/lost_cart/report', @bs_mnu_online_store_id, 0, 0, 4),
                    ("bs_mnu_online_store.mnu_config.general", 1, @bs_mnu_config_id, '/goto?owner=market_admin&url=/admin/config/general', @bs_mnu_online_store_id, 0, 0, 1),
                    ("bs_mnu_online_store.mnu_config.store", 1, @bs_mnu_config_id, '/goto?owner=market_admin&url=/admin/config/market', @bs_mnu_online_store_id, 0, 0, 2),
                    ("bs_mnu_online_store.mnu_config.payments", 1, @bs_mnu_config_id, '/goto?owner=market_admin&url=/admin/config/payments', @bs_mnu_online_store_id, 0, 0, 3),
                    ("bs_mnu_online_store.mnu_config.checkout", 1, @bs_mnu_config_id, '/goto?owner=market_admin&url=/admin/config/checkout', @bs_mnu_online_store_id, 0, 0, 4),
                    ("bs_mnu_online_store.mnu_config.plugins", 1, @bs_mnu_config_id, '/goto?owner=extensions&url=/admin/config/bsale_store', @bs_mnu_online_store_id, 0, 0, 5),
                    ("bs_mnu_online_store.mnu_config.marketing_events", 1, @bs_mnu_config_id, '/goto?owner=marketing_events&url=/admin/events', @bs_mnu_online_store_id, 0, 0, 6);
            """
            r = self._execute_query(query, (), False, True)
            if not r["success"]:
                raise ValueError(r["error"])

            resp["success"] = True
            resp["row_affected"] = r["row_affected"]
        except ValueError as error:
            resp["error"] = str(error)
            resp["success"] = False
        return resp

    # TIENDA EN LINEA       
    def set_menu_online_store_mp(self):
        resp = {}
        try:
            query = """
                -- MENU "TIENDA EN LINEA MP"
                SET @bs_mnu_online_store_mp_id = (
                    SELECT m_id
                    FROM bs_menu
                    WHERE m_name = 'bs_mnu_online_store_mp_v2'
                );

                INSERT INTO `bs_menu_link`
                    (`ml_name`, `ml_active`, `ml_asociate`, `ml_url`, `m_id`, `ml_is_dropdown`, `id_accion_modulo`, `ml_order`)
                VALUES 
                    ("bs_mnu_online_store_mp.shortcuts", 1, 0, NULL, @bs_mnu_online_store_mp_id, 1, 0, 1);
                    
                SET @bs_mnu_shortcuts_id = (
                    SELECT ml_id
                    FROM bs_menu_link
                    WHERE ml_name = 'bs_mnu_online_store_mp.shortcuts'
                );

                -- PEDIDOS WEB
                INSERT INTO `bs_temporary`
                    (`ml_name`, `ml_active`, `ml_asociate`, `ml_url`, `m_id`, `ml_is_dropdown`, `id_accion_modulo`, `ml_order`)
                VALUES 
                    ("bs_mnu_online_store_mp.orders", 1, 0, '/goto?owner=document&url=/orders/status/', @bs_mnu_online_store_mp_id, 0, @accion_modulo_pos_online_orders_new_id, 2);

                -- COLLECCIONES
                INSERT INTO `bs_temporary`
                    (`ml_name`, `ml_active`, `ml_asociate`, `ml_url`, `m_id`, `ml_is_dropdown`, `id_accion_modulo`, `ml_order`)
                VALUES 
                    ("bs_mnu_online_store_mp.collection", 1, 0, "/goto?owner=market_admin&url=/admin/config/collections", @bs_mnu_online_store_mp_id, 0, 0, 3);

                -- AJUSTES
                INSERT INTO `bs_menu_link` 
                    (`ml_name`, `ml_active`, `ml_asociate`, `ml_url`, `m_id`, `ml_is_dropdown`, `id_accion_modulo`, `ml_order`)
                VALUES
                    ("bs_mnu_online_store_mp.config", 1, 0, NULL, @bs_mnu_online_store_mp_id, 1, 0, 4);
                    
                SET @bs_mnu_config_id = (
                    SELECT ml_id
                    FROM bs_menu_link
                    WHERE ml_name = 'bs_mnu_online_store_mp.config'
                    AND m_id = @bs_mnu_online_store_mp_id
                );

                INSERT INTO `bs_temporary` 
                    (`ml_name`, `ml_active`, `ml_asociate`, `ml_url`, `m_id`, `ml_is_dropdown`, `id_accion_modulo`, `ml_order`)
                VALUES
                    ("bs_mnu_online_store_mp.mnu_shortcuts.productweb", 1, @bs_mnu_shortcuts_id, '/goto?owner=product_admin&url=/admin/products', @bs_mnu_online_store_mp_id, 0, @accion_modulo_maes_products_new_id, 1),
                    ("bs_mnu_online_store_mp.mnu_shortcuts.productweb", 1, @bs_mnu_shortcuts_id, '/goto?owner=product_admin&url=/admin/products', @bs_mnu_online_store_mp_id, 0, @accion_modulo_maes_products_edit_id, 1),
                    ("bs_mnu_online_store_mp.mnu_shortcuts.collection", 1, @bs_mnu_shortcuts_id, '/goto?owner=market_admin&url=/admin/config/collections', @bs_mnu_online_store_mp_id, 0, 0, 2),
                    ("bs_mnu_online_store_mp.mnu_shortcuts.orders", 1, @bs_mnu_shortcuts_id, '/goto?owner=document&url=/orders/status/', @bs_mnu_online_store_mp_id, 0, @accion_modulo_pos_online_orders_new_id, 3),
                    ("bs_mnu_online_store_mp.mnu_config.general", 1, @bs_mnu_config_id , '/goto?owner=market_admin&url=/admin/config/general', @bs_mnu_online_store_mp_id, 0, 0, 1),
                    ("bs_mnu_online_store_mp.mnu_config.store", 1, @bs_mnu_config_id , '/goto?owner=market_admin&url=/admin/config/market', @bs_mnu_online_store_mp_id, 0, 0, 2),
                    ("bs_mnu_online_store_mp.mnu_config.payment", 1, @bs_mnu_config_id , '/goto?owner=market_admin&url=/admin/config/payments', @bs_mnu_online_store_mp_id, 0, 0, 3),
                    ("bs_mnu_online_store_mp.mnu_config.plugins", 1, @bs_mnu_config_id , '/goto?owner=extensions&url=/admin/config/bsale_store', @bs_mnu_online_store_mp_id, 0, 0, 4);
            """
            r = self._execute_query(query, (), False, True)
            if not r["success"]:
                raise ValueError(r["error"])

            resp["success"] = True
            resp["row_affected"] = r["row_affected"]
        except ValueError as error:
            resp["error"] = str(error)
            resp["success"] = False
        return resp

    # MENU MAIN       
    def set_menu_main(self):
        resp = {}
        try:
            query = """
                -- MENU "MAIN - DOCUMENTOS"
                SET @bs_mnu_main_id = (
                    SELECT m_id
                    FROM bs_menu
                    WHERE m_name = 'bs_mnu_main_v2'
                );

                SET @accion_modulo_pos_venta_new_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.pos.venta.new' LIMIT 1);
                SET @accion_modulo_pos_venta_edit_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.pos.venta.edit' LIMIT 1);
                SET @accion_modulo_pos_genera_documentos_new_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.pos.genera_documentos.new' LIMIT 1);
                SET @accion_modulo_pos_venta_pdv_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.pos.venta.pdv' LIMIT 1);
                SET @accion_modulo_pos_pago_cierre_mes_new_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.pos.cierre_mes.new' LIMIT 1);
                SET @accion_modulo_pos_pago_cierre_mes_edit_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.pos.cierre_mes.edit' LIMIT 1);
                SET @accion_modulo_pos_libro_mensual_new_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.pos.libro_mensual.new' LIMIT 1);
                SET @accion_modulo_pos_pago_creditos_new_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.pos.pago_creditos.new' LIMIT 1);
                SET @accion_modulo_pos_pago_creditos_edit_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.pos.pago_creditos.edit' LIMIT 1);
                SET @accion_modulo_pos_devoluciones_new_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.pos.devoluciones.new' LIMIT 1);
                SET @accion_modulo_pos_devoluciones_edit_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.pos.devoluciones.edit' LIMIT 1);
                SET @accion_modulo_pos_online_orders_new_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.pos.online_orders.new' LIMIT 1);
                SET @accion_modulo_pos_despacho_new_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.pos.despacho.new' LIMIT 1);
                SET @accion_modulo_pos_despacho_edit_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.pos.despacho.edit' LIMIT 1);
                SET @accion_modulo_pos_devoluciones_monto_new_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.pos.devoluciones_monto.new' LIMIT 1);
                SET @accion_modulo_pos_retiro_abono_cliente_new_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.pos.retiro_abono_cliente.new' LIMIT 1);
                SET @accion_modulo_pos_cession_dte_new_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.pos.cesion_dte.new' LIMIT 1);
                SET @accion_modulo_pos_abono_cliente_new_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.pos.abono_cliente.new' LIMIT 1);

                INSERT INTO `bs_temporary` 
                    (`ml_name`, `ml_active`, `ml_asociate`, `ml_url`, `m_id`, `ml_is_dropdown`, `id_accion_modulo`, `ml_order`)
                VALUES
                    ("bs_mnu_main.documents", 1, 0, '/goto?owner=bsale&url=/documents', @bs_mnu_main_id, 0, @accion_modulo_pos_venta_new_id, 1),
                    ("bs_mnu_main.documents", 1, 0, '/goto?owner=bsale&url=/documents', @bs_mnu_main_id, 0, @accion_modulo_pos_venta_edit_id, 1),
                    ("bs_mnu_main.documents", 1, 0, '/goto?owner=bsale&url=/documents', @bs_mnu_main_id, 0, @accion_modulo_pos_venta_pdv_id, 1),
                    ("bs_mnu_main.documents", 1, 0, '/goto?owner=bsale&url=/documents', @bs_mnu_main_id, 0, @accion_modulo_pos_genera_documentos_new_id, 1),
                    ("bs_mnu_main.documents", 1, 0, '/goto?owner=bsale&url=/documents', @bs_mnu_main_id, 0, @accion_modulo_pos_pago_cierre_mes_new_id, 1),
                    ("bs_mnu_main.documents", 1, 0, '/goto?owner=bsale&url=/documents', @bs_mnu_main_id, 0, @accion_modulo_pos_pago_cierre_mes_edit_id, 1),
                    ("bs_mnu_main.documents", 1, 0, '/goto?owner=bsale&url=/documents', @bs_mnu_main_id, 0, @accion_modulo_pos_libro_mensual_new_id, 1),
                    ("bs_mnu_main.documents", 1, 0, '/goto?owner=bsale&url=/documents', @bs_mnu_main_id, 0, @accion_modulo_pos_pago_creditos_new_id, 1),
                    ("bs_mnu_main.documents", 1, 0, '/goto?owner=bsale&url=/documents', @bs_mnu_main_id, 0, @accion_modulo_pos_pago_creditos_edit_id, 1),
                    ("bs_mnu_main.documents", 1, 0, '/goto?owner=bsale&url=/documents', @bs_mnu_main_id, 0, @accion_modulo_pos_devoluciones_new_id, 1),
                    ("bs_mnu_main.documents", 1, 0, '/goto?owner=bsale&url=/documents', @bs_mnu_main_id, 0, @accion_modulo_pos_devoluciones_edit_id, 1),
                    ("bs_mnu_main.documents", 1, 0, '/goto?owner=bsale&url=/documents', @bs_mnu_main_id, 0, @accion_modulo_pos_online_orders_new_id, 1),
                    ("bs_mnu_main.documents", 1, 0, '/goto?owner=bsale&url=/documents', @bs_mnu_main_id, 0, @accion_modulo_pos_despacho_new_id, 1),
                    ("bs_mnu_main.documents", 1, 0, '/goto?owner=bsale&url=/documents', @bs_mnu_main_id, 0, @accion_modulo_pos_despacho_edit_id, 1),
                    ("bs_mnu_main.documents", 1, 0, '/goto?owner=bsale&url=/documents', @bs_mnu_main_id, 0, @accion_modulo_pos_devoluciones_monto_new_id, 1),
                    ("bs_mnu_main.documents", 1, 0, '/goto?owner=bsale&url=/documents', @bs_mnu_main_id, 0, @accion_modulo_pos_retiro_abono_cliente_new_id, 1),
                    ("bs_mnu_main.documents", 1, 0, '/goto?owner=bsale&url=/documents', @bs_mnu_main_id, 0, @accion_modulo_pos_cession_dte_new_id, 1),
                    ("bs_mnu_main.documents", 1, 0, '/goto?owner=bsale&url=/documents', @bs_mnu_main_id, 0, @accion_modulo_pos_abono_cliente_new_id, 1);

                -- MENU "MAIN - PRODUCTOS"
                SET @accion_modulo_maes_products_new_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.maes.productos.new' LIMIT 1);
                SET @accion_modulo_maes_products_edit_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.maes.productos.edit' LIMIT 1);
                SET @accion_modulo_maes_tipo_product_new_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.maes.tipo_producto.new' LIMIT 1);
                SET @accion_modulo_maes_tipo_product_edit_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.maes.tipo_producto.edit' LIMIT 1);
                SET @accion_modulo_maes_kit_new_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.maes.plan.new' LIMIT 1);
                SET @accion_modulo_maes_kit_edit_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.maes.plan.edit' LIMIT 1);
                SET @accion_modulo_maes_lista_precios_new_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.maes.lista_precios.new' LIMIT 1);
                SET @accion_modulo_maes_lista_precios_edit_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.maes.lista_precios.edit' LIMIT 1);
                SET @accion_modulo_reports_stock_view_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.reports.stock.view' LIMIT 1);
                SET @accion_modulo_maes_stock_new_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.maes.stock.new' LIMIT 1);
                SET @accion_modulo_maes_stock_edit_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.maes.stock.edit' LIMIT 1);
                SET @accion_modulo_maes_inventario_new_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.maes.inventario.new' LIMIT 1);
                SET @accion_modulo_pos_update_costos_new_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.pos.update_costos.new' LIMIT 1);
                SET @accion_modulo_maes_descuentos_new_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.maes.descuentos.new' LIMIT 1);
                SET @accion_modulo_maes_descuentos_edit_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.maes.descuentos.edit' LIMIT 1);
                SET @accion_modulo_maes_cupones_new_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.maes.cupones.new' LIMIT 1);
                SET @accion_modulo_maes_cupones_edit_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.maes.cupones.edit' LIMIT 1);

                INSERT INTO `bs_temporary` 
                    (`ml_name`, `ml_active`, `ml_asociate`, `ml_url`, `m_id`, `ml_is_dropdown`, `id_accion_modulo`, `ml_order`)
                VALUES
                    ("bs_mnu_main.products", 1, 0, '/goto?owner=product_admin&url=/admin/products/shortcuts', @bs_mnu_main_id, 0, @accion_modulo_maes_products_new_id, 2),
                    ("bs_mnu_main.products", 1, 0, '/goto?owner=product_admin&url=/admin/products/shortcuts', @bs_mnu_main_id, 0, @accion_modulo_maes_products_edit_id, 2),
                    ("bs_mnu_main.products", 1, 0, '/goto?owner=product_admin&url=/admin/products/shortcuts', @bs_mnu_main_id, 0, @accion_modulo_maes_tipo_product_new_id, 2),
                    ("bs_mnu_main.products", 1, 0, '/goto?owner=product_admin&url=/admin/products/shortcuts', @bs_mnu_main_id, 0, @accion_modulo_maes_tipo_product_edit_id, 2),
                    ("bs_mnu_main.products", 1, 0, '/goto?owner=product_admin&url=/admin/products/shortcuts', @bs_mnu_main_id, 0, @accion_modulo_maes_kit_new_id, 2),
                    ("bs_mnu_main.products", 1, 0, '/goto?owner=product_admin&url=/admin/products/shortcuts', @bs_mnu_main_id, 0, @accion_modulo_maes_kit_edit_id, 2),
                    ("bs_mnu_main.products", 1, 0, '/goto?owner=product_admin&url=/admin/products/shortcuts', @bs_mnu_main_id, 0, @accion_modulo_maes_lista_precios_new_id, 2),
                    ("bs_mnu_main.products", 1, 0, '/goto?owner=product_admin&url=/admin/products/shortcuts', @bs_mnu_main_id, 0, @accion_modulo_maes_lista_precios_edit_id, 2),
                    ("bs_mnu_main.products", 1, 0, '/goto?owner=product_admin&url=/admin/products/shortcuts', @bs_mnu_main_id, 0, @accion_modulo_reports_stock_view_id, 2),
                    ("bs_mnu_main.products", 1, 0, '/goto?owner=product_admin&url=/admin/products/shortcuts', @bs_mnu_main_id, 0, @accion_modulo_maes_stock_new_id, 2),
                    ("bs_mnu_main.products", 1, 0, '/goto?owner=product_admin&url=/admin/products/shortcuts', @bs_mnu_main_id, 0, @accion_modulo_maes_stock_edit_id, 2),
                    ("bs_mnu_main.products", 1, 0, '/goto?owner=product_admin&url=/admin/products/shortcuts', @bs_mnu_main_id, 0, @accion_modulo_maes_inventario_new_id, 2),
                    ("bs_mnu_main.products", 1, 0, '/goto?owner=product_admin&url=/admin/products/shortcuts', @bs_mnu_main_id, 0, @accion_modulo_pos_update_costos_new_id, 2),
                    ("bs_mnu_main.products", 1, 0, '/goto?owner=product_admin&url=/admin/products/shortcuts', @bs_mnu_main_id, 0, @accion_modulo_maes_descuentos_new_id, 2),
                    ("bs_mnu_main.products", 1, 0, '/goto?owner=product_admin&url=/admin/products/shortcuts', @bs_mnu_main_id, 0, @accion_modulo_maes_descuentos_edit_id, 2),
                    ("bs_mnu_main.products", 1, 0, '/goto?owner=product_admin&url=/admin/products/shortcuts', @bs_mnu_main_id, 0, @accion_modulo_maes_cupones_new_id, 2),
                    ("bs_mnu_main.products", 1, 0, '/goto?owner=product_admin&url=/admin/products/shortcuts', @bs_mnu_main_id, 0, @accion_modulo_maes_cupones_edit_id, 2);

                -- MENU "MAIN - CLIENTES"
                SET @accion_modulo_maes_clientes_new_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.maes.clientes.new' LIMIT 1);
                SET @accion_modulo_maes_clientes_edit_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.maes.clientes.edit' LIMIT 1);
                SET @accion_modulo_maes_bpoints_catalogo_new_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.maes.bpoints_catalogo.new' LIMIT 1);
                SET @accion_modulo_maes_bpoints_edit_edit_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.maes.bpoints_edit.edit' LIMIT 1);

                INSERT INTO `bs_temporary` 
                    (`ml_name`, `ml_active`, `ml_asociate`, `ml_url`, `m_id`, `ml_is_dropdown`, `id_accion_modulo`, `ml_order`)
                VALUES
                    ("bs_mnu_main.clients", 1, 0, '/goto?owner=bsale&url=/admin/clientes', @bs_mnu_main_id, 0, @accion_modulo_maes_clientes_new_id, 3),
                    ("bs_mnu_main.clients", 1, 0, '/goto?owner=bsale&url=/admin/clientes', @bs_mnu_main_id, 0, @accion_modulo_maes_clientes_edit_id, 3),
                    ("bs_mnu_main.clients", 1, 0, '/goto?owner=bsale&url=/admin/clientes', @bs_mnu_main_id, 0, @accion_modulo_maes_bpoints_catalogo_new_id, 3),
                    ("bs_mnu_main.clients", 1, 0, '/goto?owner=bsale&url=/admin/clientes', @bs_mnu_main_id, 0, @accion_modulo_maes_bpoints_edit_edit_id, 3);

                -- MENU "MAIN - PUNTO DE VENTA"
                SET @accion_modulo_pos_devoluciones_pdv_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.pos.devoluciones.pdv' LIMIT 1);
                SET @accion_modulo_pos_pago_creditos_pdv_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.pos.pago_creditos.pdv' LIMIT 1);
                SET @accion_modulo_pos_despacho_pdv_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.pos.despacho.pdv' LIMIT 1);
                SET @accion_modulo_pos_apertura_caja_pdv_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.pos.apertura_caja.pdv' LIMIT 1);
                SET @accion_modulo_pos_reprint_doc_pdv_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.pos.reprint_doc.pdv' LIMIT 1); 
                SET @pos_status = (SELECT ml_active FROM bs_menu_link WHERE ml_name = 'bs_mnu_main.pos' LIMIT 1);

                INSERT INTO `bs_temporary` 
                    (`ml_name`, `ml_active`, `ml_asociate`, `ml_url`, `m_id`, `ml_is_dropdown`, `id_accion_modulo`, `ml_order`)
                VALUES
                    ("bs_mnu_main.pos", @pos_status, 0, '/goto?owner=bsale&url=/mobile', @bs_mnu_main_id, 0, @accion_modulo_pos_venta_pdv_id, 4),
                    ("bs_mnu_main.pos", @pos_status, 0, '/goto?owner=bsale&url=/mobile', @bs_mnu_main_id, 0, @accion_modulo_pos_devoluciones_pdv_id, 4),
                    ("bs_mnu_main.pos", @pos_status, 0, '/goto?owner=bsale&url=/mobile', @bs_mnu_main_id, 0, @accion_modulo_pos_pago_creditos_pdv_id, 4),
                    ("bs_mnu_main.pos", @pos_status, 0, '/goto?owner=bsale&url=/mobile', @bs_mnu_main_id, 0, @accion_modulo_pos_despacho_pdv_id, 4),
                    ("bs_mnu_main.pos", @pos_status, 0, '/goto?owner=bsale&url=/mobile', @bs_mnu_main_id, 0, @accion_modulo_pos_apertura_caja_pdv_id, 4),
                    ("bs_mnu_main.pos", @pos_status, 0, '/goto?owner=bsale&url=/mobile', @bs_mnu_main_id, 0, @accion_modulo_pos_reprint_doc_pdv_id, 4);

                -- MENU "MAIN-TIENDA EN LINEA"
                SET @bs_mnu_online_store_status = (SELECT ml_active FROM bs_menu_link WHERE ml_name = 'bs_mnu_main.online_store' LIMIT 1);
                SET @accion_modulo_pos_online_store_new_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.pos.online_store.new' LIMIT 1);

                INSERT INTO `bs_temporary` 
                    (`ml_name`, `ml_active`, `ml_asociate`, `ml_url`, `m_id`, `ml_is_dropdown`, `id_accion_modulo`, `ml_order`)
                VALUES
                    ("bs_mnu_main.online_store", @bs_mnu_online_store_status, 0, '/goto?owner=market&url=/admin/index', @bs_mnu_main_id, 0, @accion_modulo_pos_online_store_new_id, 5);

                -- MENU "MAIN-REPORTES"
                SET @accion_modulo_reports_checks_to_cash_view_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.reports.checks_to_cash.view' LIMIT 1);
                SET @accion_modulo_reports_client_debt_view_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.reports.client_debt.view' LIMIT 1);
                SET @accion_modulo_reports_deposits_view_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.reports.deposits.view' LIMIT 1);
                SET @accion_modulo_reports_discount_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.reports.discount.view' LIMIT 1);
                SET @accion_modulo_reports_dynamic_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.reports.dynamic.view' LIMIT 1);
                SET @accion_modulo_reports_libro_mensual_view_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.reports.libro_mensual.view' LIMIT 1);
                SET @accion_modulo_reports_my_sales_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.reports.my_sales.view' LIMIT 1);
                SET @accion_modulo_reports_pending_docs_view_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.reports.pending_docs.view' LIMIT 1);
                SET @accion_modulo_reports_pos_cash_flow_view_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.reports.pos_cash_flow.view' LIMIT 1);
                SET @accion_modulo_reports_reprint_view_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.reports.reprint.view' LIMIT 1);
                SET @accion_modulo_reports_return_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.reports.returns.view' LIMIT 1);
                SET @accion_modulo_reports_sent_documents_view_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.reports.sent_documents.view' LIMIT 1);
                SET @accion_modulo_reports_shipping_view_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.reports.shipping.view' LIMIT 1);
                SET @accion_modulo_reports_third_party_docs_view_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.reports.third_party_docs.view' LIMIT 1);
                SET @accion_modulo_reports_coupons_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.reports.coupons.view' LIMIT 1);
                SET @accion_modulo_maes_bpoints_new_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.maes.bpoints.new' LIMIT 1);
                SET @accion_modulo_reports_documents_by_currency_view_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.reports.documents_by_currency.view' LIMIT 1);
                SET @accion_modulo_sales_overview_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.reports_v2.sales_overview.view' LIMIT 1);
                SET @accion_modulo_sales_reports_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.reports_v2.sales_reports.view' LIMIT 1);
                SET @accion_modulo_pos_online_orders_new_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.pos.online_orders.new' LIMIT 1);

                INSERT INTO `bs_temporary` 
                    (`ml_name`, `ml_active`, `ml_asociate`, `ml_url`, `m_id`, `ml_is_dropdown`, `id_accion_modulo`, `ml_order`)
                VALUES
                    ("bs_mnu_main.reports", 1, 0, '/goto?owner=bsale&url=/reports', @bs_mnu_main_id, 0, @accion_modulo_reports_checks_to_cash_view_id, 6),
                    ("bs_mnu_main.reports", 1, 0, '/goto?owner=bsale&url=/reports', @bs_mnu_main_id, 0, @accion_modulo_reports_client_debt_view_id, 6),
                    ("bs_mnu_main.reports", 1, 0, '/goto?owner=bsale&url=/reports', @bs_mnu_main_id, 0, @accion_modulo_reports_deposits_view_id, 6),
                    ("bs_mnu_main.reports", 1, 0, '/goto?owner=bsale&url=/reports', @bs_mnu_main_id, 0, @accion_modulo_reports_discount_id, 6),
                    ("bs_mnu_main.reports", 1, 0, '/goto?owner=bsale&url=/reports', @bs_mnu_main_id, 0, @accion_modulo_reports_dynamic_id, 6),
                    ("bs_mnu_main.reports", 1, 0, '/goto?owner=bsale&url=/reports', @bs_mnu_main_id, 0, @accion_modulo_reports_libro_mensual_view_id, 6),
                    ("bs_mnu_main.reports", 1, 0, '/goto?owner=bsale&url=/reports', @bs_mnu_main_id, 0, @accion_modulo_reports_my_sales_id, 6),
                    ("bs_mnu_main.reports", 1, 0, '/goto?owner=bsale&url=/reports', @bs_mnu_main_id, 0, @accion_modulo_reports_pending_docs_view_id, 6),
                    ("bs_mnu_main.reports", 1, 0, '/goto?owner=bsale&url=/reports', @bs_mnu_main_id, 0, @accion_modulo_reports_pos_cash_flow_view_id, 6),
                    ("bs_mnu_main.reports", 1, 0, '/goto?owner=bsale&url=/reports', @bs_mnu_main_id, 0, @accion_modulo_reports_reprint_view_id, 6),
                    ("bs_mnu_main.reports", 1, 0, '/goto?owner=bsale&url=/reports', @bs_mnu_main_id, 0, @accion_modulo_reports_return_id, 6),
                    ("bs_mnu_main.reports", 1, 0, '/goto?owner=bsale&url=/reports', @bs_mnu_main_id, 0, @accion_modulo_reports_sent_documents_view_id, 6),
                    ("bs_mnu_main.reports", 1, 0, '/goto?owner=bsale&url=/reports', @bs_mnu_main_id, 0, @accion_modulo_reports_shipping_view_id, 6),
                    ("bs_mnu_main.reports", 1, 0, '/goto?owner=bsale&url=/reports', @bs_mnu_main_id, 0, @accion_modulo_reports_stock_view_id, 6),
                    ("bs_mnu_main.reports", 1, 0, '/goto?owner=bsale&url=/reports', @bs_mnu_main_id, 0, @accion_modulo_reports_third_party_docs_view_id, 6),
                    ("bs_mnu_main.reports", 1, 0, '/goto?owner=bsale&url=/reports', @bs_mnu_main_id, 0, @accion_modulo_reports_coupons_id, 6),
                    ("bs_mnu_main.reports", 1, 0, '/goto?owner=bsale&url=/reports', @bs_mnu_main_id, 0, @accion_modulo_maes_bpoints_new_id, 6),
                    ("bs_mnu_main.reports", 1, 0, '/goto?owner=bsale&url=/reports', @bs_mnu_main_id, 0, @accion_modulo_maes_bpoints_edit_id, 6),
                    ("bs_mnu_main.reports", 1, 0, '/goto?owner=bsale&url=/reports', @bs_mnu_main_id, 0, @accion_modulo_reports_documents_by_currency_view_id, 6),
                    ("bs_mnu_main.reports", 1, 0, '/goto?owner=bsale&url=/reports', @bs_mnu_main_id, 0, @accion_modulo_sales_overview_id, 6),
                    ("bs_mnu_main.reports", 1, 0, '/goto?owner=bsale&url=/reports', @bs_mnu_main_id, 0, @accion_modulo_sales_reports_id, 6),
                    ("bs_mnu_main.reports", 1, 0, '/goto?owner=bsale&url=/reports', @bs_mnu_main_id, 0, @accion_modulo_pos_online_orders_new_id, 6);
            """
            r = self._execute_query(query, (), False, True)
            if not r["success"]:
                raise ValueError(r["error"])

            resp["success"] = True
            resp["row_affected"] = r["row_affected"]
        except ValueError as error:
            resp["error"] = str(error)
            resp["success"] = False
        return resp

    # LIMPIAR TABLA TEMPORAL
    def clear_temporary_table(self):
        resp = {}
        try:
            query = """
                DELETE FROM bs_temporary WHERE id_accion_modulo IS NULL;
                INSERT INTO bs_menu_link 
                    (`ml_name`, `ml_active`, `ml_asociate`, `ml_url`, `m_id`, `ml_is_dropdown`, `id_accion_modulo`, `ml_order`)
                SELECT 
                    `ml_name`, `ml_active`, `ml_asociate`, `ml_url`, `m_id`, `ml_is_dropdown`, `id_accion_modulo`, `ml_order` 
                FROM bs_temporary;
                DROP TEMPORARY TABLE bs_temporary;
            """
            r = self._execute_query(query, (), False, True)
            if not r["success"]:
                raise ValueError(r["error"])

            resp["success"] = True
            resp["row_affected"] = r["row_affected"]
        except ValueError as error:
            resp["error"] = str(error)
            resp["success"] = False
        return resp

    def get_country(self):
        try:
            query = 'SELECT valor FROM sys_config WHERE variable = "country";'
            r = self._execute_query(query, ())
            if r["success"] and r["data"]:
                return r["data"][0]["valor"].upper()
            return None
        except Exception as e:
            return None

    def set_bsale_landing_page(self):
        resp = {}
        landing_country_url = {
            "CL": "https://landing.bsale.cl/",
            "PE": "https://landing.bsale.pe/",
            "MX": "https://landing.bsale.mx/",
        }
        try:
            country_value = self.get_country()
            url = landing_country_url.get(country_value, "https://landing.bsale.cl/")
            query = """
                INSERT INTO `sys_config`
                    (`variable`, `valor`, `descripcion`, `es_editable`, `es_empresa`, `control_html`, `orden`, `id_modulo`)
                VALUES
                    ("landing_url", %s, NULL, 0, 0, NULL, NULL, 2);
            """
            r = self._execute_query(query, (url,), False)
            if not r["success"]:
                raise ValueError(r["error"])
            resp["success"] = True
            resp["row_affected"] = r["row_affected"]
        except ValueError as error:
            resp["error"] = str(error)
            resp["success"] = False
        return resp

    def update_bsale_help_url(self):
        resp = {}
        help_country_url = {
            "CL": "https://www.bsale.cl/sheet/como-podemos-ayudarte-v2?",
            "PE": "https://www.bsale.com.pe/sheet/como-podemos-ayudarte-v2?",
            "MX": "https://www.bsale.com.mx/sheet/como-podemos-ayudarte-v2?",
        }
        try:
            country_value = self.get_country()
            url = help_country_url.get(country_value, "https://www.bsale.cl/sheet/como-podemos-ayudarte-v2?")
            query = """
                UPDATE `sys_config`
                SET valor = %s
                WHERE variable = 'help_bsale'
            """
            r = self._execute_query(query, (url,), False)
            if not r["success"]:
                raise ValueError(r["error"])
            resp["success"] = True
            resp["row_affected"] = r["row_affected"]
        except ValueError as error:
            resp["error"] = str(error)
            resp["success"] = False
        return resp

    def set_menu_version(self, version, cpnId):
        resp = {}
        try:
            query = """
                UPDATE tbw_companies
                SET cpn_menu_version = %s
                WHERE cpn_id = %s;       
            """
            r = self._execute_query(query, (version, cpnId,), False)
            if not r["success"]:
                raise ValueError(r["error"])
            resp["success"] = True
            resp["row_affected"] = r["row_affected"]
        except ValueError as error:
            resp["error"] = str(error)
            resp["success"] = False
        return resp

    # def update_product_type(self):
    #     resp = {}
    #     try:
    #         query = """UPDATE `bs_menu_link` SET ml_name = 'bs_mnu_mae_product.my_product_type' WHERE ml_name = 'bs_mnu_mae_product.my_categories'"""
    #         r = self._execute_query(query, (), False, False)
    #         if not r["success"]:
    #             raise ValueError(r["error"])

    #         resp["success"] = True
    #         resp["row_affected"] = r["row_affected"]
    #     except ValueError as error:
    #         resp["error"] = str(error)
    #         resp["success"] = False
    #     return resp

    # def update_stock_by_product_type(self):
    #     resp = {}
    #     try:
    #         query = """
    #             SET @sys_stock_plan_active = (SELECT valor FROM sys_config WHERE variable = 'stock_plan_active' LIMIT 1);
    #             SET @sys_categoria_producto = (SELECT valor FROM sys_config WHERE variable = 'categoria_producto' LIMIT 1);
    #             SET @status_acc_mod_maes_stock_new_id = (SELECT estado_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.maes.stock.new' LIMIT 1);
    #             SET @status_acc_mod_maes_stock_edit_id = (SELECT estado_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.maes.stock.edit' LIMIT 1);
    #             SET @mnu_stock_status_evaluated = ( (@status_acc_mod_maes_stock_new_id = 0 OR @status_acc_mod_maes_stock_edit_id = 0) AND @sys_stock_plan_active AND @sys_categoria_producto <> 1);

    #             UPDATE `bs_menu_link` SET ml_active = @mnu_stock_status_evaluated WHERE ml_name in (
    #                 'bs_mnu_mae_product.stock',
    #                 'bs_mnu_mae_product.mnu_shortcuts.inventory',
    #                 'bs_mnu_mae_product.mnu_shortcuts.consumption',
    #                 'bs_mnu_mae_product.mnu_shortcuts.reception',
    #                 'bs_mnu_mae_product.mnu_shortcuts.stock_card',
    #                 'bs_mnu_mae_product.mnu_shortcuts.current_stock',
    #                 'bs_mnu_mae_product.mnu_shortcuts.cost_update',
    #                 'bs_mnu_mae_product.mnu_shortcuts.update_stock'
    #             );
    #         """
    #         r = self._execute_query(query, (), False, True)
    #         if not r["success"]:
    #             raise ValueError(r["error"])

    #         resp["success"] = True
    #         resp["row_affected"] = r["row_affected"]
    #     except ValueError as error:
    #         resp["error"] = str(error)
    #         resp["success"] = False
    #     return resp

    # def update_accion_modulo_pos_libro_mensual(self):
    #     resp = {}
    #     try:
    #         query = """
    #             SET @bs_mnu_documents_v2_id = (SELECT m_id FROM bs_menu WHERE m_name = 'bs_mnu_documents_v2');
    #             SET @accion_modulo_pos_libro_mensual_new_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.pos.libro_mensual.new' LIMIT 1);

    #             UPDATE `bs_menu_link` SET id_accion_modulo = @accion_modulo_pos_libro_mensual_new_id
    #             WHERE ml_name = 'bs_mnu_documents.mnu_more.new_book'
    #             AND m_id = @bs_mnu_documents_v2_id;
    #         """
    #         r = self._execute_query(query, (), False, True)
    #         if not r["success"]:
    #             raise ValueError(r["error"])

    #         resp["success"] = True
    #         resp["row_affected"] = r["row_affected"]
    #     except ValueError as error:
    #         resp["error"] = str(error)
    #         resp["success"] = False
    #     return resp

    # def update_accion_modulo_pos_online_orders(self):
    #     resp = {}
    #     try:
    #         query = """
    #             SET @bs_mnu_reports_v2_id = (SELECT m_id FROM bs_menu WHERE m_name = 'bs_mnu_reports_v2');
    #             SET @accion_modulo_pos_online_orders_new_id = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = 'accion_modulo.pos.online_orders.new' LIMIT 1);
    #             SET @bs_mnu_main_v2_id = (SELECT m_id FROM bs_menu WHERE m_name = 'bs_mnu_main_v2');
                
    #             INSERT INTO `bs_menu_link` 
    #                 (`ml_name`, `ml_active`, `ml_asociate`, `ml_url`, `m_id`, `ml_is_dropdown`, `id_accion_modulo`, `ml_order`)
    #             VALUES
    #                 ("bs_mnu_main.reports", 1, 0, '/goto?owner=bsale&url=/reports', @bs_mnu_main_v2_id, 0, @accion_modulo_pos_online_orders_new_id, 6);

    #             UPDATE `bs_menu_link` SET id_accion_modulo = @accion_modulo_pos_online_orders_new_id
    #             WHERE ml_name IN ('reports.lost_cart', 'reports.market_payment')
    #             AND m_id = @bs_mnu_reports_v2_id;
    #         """
    #         r = self._execute_query(query, (), False, True)
    #         if not r["success"]:
    #             raise ValueError(r["error"])

    #         resp["success"] = True
    #         resp["row_affected"] = r["row_affected"]
    #     except ValueError as error:
    #         resp["error"] = str(error)
    #         resp["success"] = False
    #     return resp
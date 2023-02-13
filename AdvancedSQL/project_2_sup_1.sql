WITH 
    cookie_recipes as (
        select 
            recipe_id
            ,   recipe_name
            ,   ingredients
        from vk_data.chefs.recipe
        where lower(recipe_name) in (
                'birthday cookie'
                ,   'a perfect sugar cookie'
                ,   'honey oatmeal raisin cookies'
                ,   'frosted lemon cookies'
                ,   'snickerdoodles cinnamon cookies')
),
    
    ingredients_recipe as (
        select 
            recipe_id
            ,   recipe_name
            ,   flat_ingredients.index
            ,   trim(upper(replace(flat_ingredients.value, '"', ''))) as ingredient
        from cookie_recipes
        , table(flatten(ingredients)) as flat_ingredients
),

    ingredient_format as (
        select 
            trim(upper(split_part(ingredient_name, ',', 0))) as main_ingredient
            ,   calories
            ,   cast(replace(total_fat, 'g', '') as int) as total_fat
        from vk_data.resources.nutrition
),
        
    nutrition as (
        select 
            main_ingredient
            ,   max(calories) as calories
            ,   max(total_fat) as total_fat
        from ingredient_format 
        group by main_ingredient
), 

    ingredients_nutrition as (
        select 
            ingredients_recipe.recipe_id
            ,   ingredients_recipe.recipe_name
            ,   ingredients_recipe.ingredient
            ,   nutrition.calories
            ,   nutrition.total_fat
        from ingredients_recipe
        left join nutrition on ingredients_recipe.ingredient = nutrition.main_ingredient
),

    final as (
        select
            recipe_name
            ,   sum(calories) as total_calories
            ,   sum(total_fat) as total_fat
        from ingredients_nutrition
        group by recipe_name
        order by recipe_name
)

select * from final;
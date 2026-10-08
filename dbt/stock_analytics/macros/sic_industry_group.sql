{# SIC major groups, OSHA 1987 SIC Manual: https://www.osha.gov/data/sic-manual.
   This is a static taxonomy, not a replacement claim for GICS sectors. #}
{% macro sic_industry_group(code) %}
CASE LEFT(LPAD({{ code }}, 4, '0'), 2)
{% set groups = {
 '01':'Agricultural crops','02':'Livestock','07':'Agricultural services','08':'Forestry','09':'Fishing and hunting',
 '10':'Metal mining','12':'Coal mining','13':'Oil and gas extraction','14':'Nonmetallic mineral mining',
 '15':'Building construction','16':'Heavy construction','17':'Construction trades',
 '20':'Food products','21':'Tobacco','22':'Textiles','23':'Apparel manufacturing','24':'Lumber and wood',
 '25':'Furniture','26':'Paper','27':'Printing and publishing','28':'Chemicals','29':'Petroleum refining',
 '30':'Rubber and plastics','31':'Leather','32':'Stone glass and concrete','33':'Primary metals',
 '34':'Fabricated metals','35':'Industrial machinery and computers','36':'Electrical and electronic equipment',
 '37':'Transportation equipment','38':'Instruments and medical equipment','39':'Other manufacturing',
 '40':'Railroads','41':'Passenger ground transportation','42':'Trucking and warehousing','43':'Postal services',
 '44':'Water transportation','45':'Air transportation','46':'Pipelines','47':'Transportation services',
 '48':'Communications','49':'Utilities','50':'Durable goods wholesale','51':'Nondurable goods wholesale',
 '52':'Building materials retail','53':'General merchandise retail','54':'Food retail','55':'Automotive retail',
 '56':'Apparel retail','57':'Home furnishings retail','58':'Restaurants','59':'Other retail',
 '60':'Depository institutions','61':'Nondepository credit','62':'Securities and commodity services',
 '63':'Insurance carriers','64':'Insurance agencies','65':'Real estate','67':'Investment offices',
 '70':'Lodging','72':'Personal services','73':'Business services','75':'Automotive services','76':'Repair services',
 '78':'Motion pictures','79':'Recreation','80':'Health services','81':'Legal services','82':'Education',
 '83':'Social services','84':'Museums','86':'Membership organizations','87':'Engineering and professional services',
 '88':'Private households','89':'Other services','91':'Government administration','92':'Justice and public safety',
 '93':'Public finance','94':'Human resource administration','95':'Environmental administration',
 '96':'Economic administration','97':'National security','99':'Unclassified establishments'
} %}
{% for key, label in groups.items() %}
    WHEN '{{ key }}' THEN 'SIC {{ key }} - {{ label }}'
{% endfor %}
    ELSE 'Unknown'
END
{% endmacro %}

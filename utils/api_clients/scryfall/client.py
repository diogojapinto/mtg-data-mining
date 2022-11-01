"""Module responsible for fetching and caching data from Scryfall API."""

from datetime import datetime, timedelta
import requests

from box import Box
from requests_cache import CachedSession
import pandas as pd

class ScryfallClient:
    """
    Scryfall client.

    Uses caching for the API responses, and returns results as `pandas` objects.
    """

    cache_folder = './data/scryfall'

    simplified_columns = [
        'id',
        'name',
        'mana_cost',
        'cmc',
        'type_line',
        'power',
        'toughness',
        'colors',
        'color_indicator',
        'color_identity',
        'rarity',
        'oracle_text',
        'keywords',
        'produced_mana',
        'image_uris',
        'flavor_text',
        'card_faces',
        'all_parts',
        'legalities',
        'released_at',
        'set',
        'set_name',
        'set_type',
        'artist',
        'prices',
    ]

    def __init__(self) -> None:
        # Cache API responses with requests_cache
        self.session = CachedSession(
            self.cache_folder,
            backend='filesystem',
            serializer='json',
            expire_after=timedelta(days=180),
            cache_control=True,
            allowable_codes=[200],
            allowable_method=['GET'],
            stale_if_error=False
        )

    def search_by_query(
        self,
        query: str,
        unique: str = None,
        order: str = None,
        direction: str = None,
        include_extras: bool = False,
        include_multilingual: bool = False,
        include_variations: bool = False,
        return_simplified_fields: bool = True
    ):
        """
        Search Scryfall via fulltext search string.

        Returns a list of cards found using a fulltext search string.

        Reference for fulltext search string syntax at https://scryfall.com/docs/syntax.
        API endpoint documentation at https://scryfall.com/docs/api/cards/search.

        Parameters
        ----------
        query : str
            A fulltext search query.
        unique : str, optional
            The strategy for omitting similar cards.
        order : str, optional
            The method to sort returned cards.
        direction : str
            The direction to sort cards.
        include_extras : bool, default False
            If true, extra cards (tokens, planes, etc) will be included.
        include_multilingual : bool, default False
            If true, cards in every language supported by Scryfall will be included.
        include_variations : bool, default False
            If true, rare care variants will be included.
        return_simplified_fields : bool, default True
            If true, excludes rarely-used fields from the output.

        Returns
        -------
        search_results : pandas.DataFrame
            The cards resulting from the search.
            For full documentation on columns returned, see https://scryfall.com/docs/api/cards.
            If `return_simplified_fields` is true, the output
            is reduced to the following columns:
                id : str
                    An unique ID for the card in Scryfall's database.
                name : str
                    The name of the card.
                mana_cost : str
                    The mana cost for the card
                cmc : str
                    The card's converted mana cost.
                type_line : str
                    The type line of the card.
                power : str
                    The card's power, if any.
                toughness : str
                    The card's toughness, if any.
                colors : str
                    The card's colors, if the overall card has colors defined by the rules.
                color_indicator : str
                    The colors in this card's color indicator, if any.
                color_identity : str
                    The card's color identity.
                rarity : str
                    The card's rarity.
                oracle_text : str
                    The Oracle text for this card, if any.
                keywords : List[str]
                    An array of keywords that this card uses.
                produced_mana : List[str]
                    Colors of mana that this card could produce.
                image_uris : box.Box
                    An object listing available imagery for this card.
                flavor_text : str
                    The flavor text printed on this face, if any.
                card_faces : List[box.Box]
                    Multiface cards have a `card_faces` property containing at least
                    two Card Face objects.
                    For further details, see https://scryfall.com/docs/api/cards#card-face-objects
                all_parts : List[box.Box]
                    If this card is closely related to other cards, this property
                    will be an array with Related Card Objects.
                    For futher details, see https://scryfall.com/docs/api/cards#related-card-objects
                legalities : box.Box
                    An object describing the legality of this card across play formats.
                released_at : datetime.date
                    The date this card was first released.
                set : str
                    The card's set code.
                set_name : str
                    The card's full set name.
                set_type : str
                    The type of set this printing is in.
                artist : str
                    The name of the illustrator of this card face.
                prices : box.Box
                    An object containing daily price information for this card.

        Examples
        --------
        >>> search_by_query(
                query='c:red pow:3',
                order='cmc'
            )
        """
        # Prepare and perform API request
        url = 'https://api.scryfall.com/cards/search'
        params = {
            'q': query,
            'unique': unique,
            'order': order,
            'dir': direction,
            'include_extras': include_extras,
            'include_multilingual': include_multilingual,
            'include_variations': include_variations,
            'page': 1,
            'format': 'json',
            'pretty': False
        }

        # Iterate and accumulate the pages of results
        has_finished = False
        search_results_accum = []
        while not has_finished:
            response = Box(self.session.get(url=url, params=params).json())

            if response.object == 'error':
                raise Exception(response.details)

            search_results_accum.extend(response.data)

            if not response.has_more:
                has_finished = True
            else:
                url = response.next_page
                params = {}

        # Prepare dataframe
        search_results = pd.DataFrame(search_results_accum)
        search_results.loc[:, 'released_at'] = search_results.released_at.apply(
            lambda day: datetime.strptime(day, '%Y-%m-%d')
        )

        # If requested, simplify the output results
        if return_simplified_fields:
            search_results = search_results.loc[:, self.simplified_columns]

        return search_results

    def search_by_name(
        self,
        exact: str = None,
        fuzzy: str = None,
        set_code: str = None,
        return_simplified_fields: bool = True
    ) -> Box:
        """
        Search Scryfall via exact or fuzzy card name.

        Returns a single card object, or error if no card or multiple cards
        are found for the name.

        API endpoint documentation at https://scryfall.com/docs/api/cards/named.

        Parameters
        ----------
        exact : str
            The exact card name to search for, case insenstive.
        fuzzy : str
            A fuzzy card name to search for.
        set_code : str
            A set code to limit the search to one set.
        return_simplified_fields : bool, default True
            If true, excludes rarely-used fields from the output.

        Returns
        -------
        search_result : box.Box
            The cards resulting from the search.
            For full documentation on columns returned, see https://scryfall.com/docs/api/cards.
            If `return_simplified_fields` is true, the output
            is reduced to the following columns:
                id : str
                    An unique ID for the card in Scryfall's database.
                name : str
                    The name of the card.
                mana_cost : str
                    The mana cost for the card
                cmc : str
                    The card's converted mana cost.
                type_line : str
                    The type line of the card.
                power : str
                    The card's power, if any.
                toughness : str
                    The card's toughness, if any.
                colors : str
                    The card's colors, if the overall card has colors defined by the rules.
                color_indicator : str
                    The colors in this card's color indicator, if any.
                color_identity : str
                    The card's color identity.
                rarity : str
                    The card's rarity.
                oracle_text : str
                    The Oracle text for this card, if any.
                keywords : List[str]
                    An array of keywords that this card uses.
                produced_mana : List[str]
                    Colors of mana that this card could produce.
                image_uris : box.Box
                    An object listing available imagery for this card.
                flavor_text : str
                    The flavor text printed on this face, if any.
                card_faces : List[box.Box]
                    Multiface cards have a `card_faces` property containing at least
                    two Card Face objects.
                    For further details, see https://scryfall.com/docs/api/cards#card-face-objects
                all_parts : List[box.Box]
                    If this card is closely related to other cards, this property
                    will be an array with Related Card Objects.
                    For futher details, see https://scryfall.com/docs/api/cards#related-card-objects
                legalities : box.Box
                    An object describing the legality of this card across play formats.
                released_at : datetime.date
                    The date this card was first released.
                set : str
                    The card's set code.
                set_name : str
                    The card's full set name.
                set_type : str
                    The type of set this printing is in.
                artist : str
                    The name of the illustrator of this card face.
                prices : box.Box
                    An object containing daily price information for this card.

        Examples
        --------
        >>> search_by_name(
                fuzzy='jac bel'
            )
        """

        # Check if at least one search parameter was provided
        if (exact is None and fuzzy is None) or (exact is not None and fuzzy is not None):
            raise Exception("Please provide one of `exact` or `fuzzy` parameters.")

        # Prepare and perform API request
        url = 'https://api.scryfall.com/cards/named'
        params = {
            'exact': exact,
            'fuzzy': fuzzy,
            'format': 'json',
            'set': set_code,
            'pretty': False
        }
        search_result = Box(self.session.get(url=url, params=params).json())

        if search_result.object == 'error':
            raise Exception(search_result.details)

        # If requested, simplify the output results
        if return_simplified_fields:
            search_result = Box({key: search_result[key] for key in self.simplified_columns if key in search_result})

        return search_result
